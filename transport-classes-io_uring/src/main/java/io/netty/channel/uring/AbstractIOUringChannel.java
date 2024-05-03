/*
 * Copyright 2022 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.channel.uring;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.AbstractChannel;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPromise;
import io.netty.channel.ConnectTimeoutException;
import io.netty.channel.unix.Buffer;
import io.netty.channel.unix.Errors;
import io.netty.channel.unix.FileDescriptor;
import io.netty.channel.unix.UnixChannel;
import io.netty.channel.unix.UnixChannelUtil;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.collection.LongObjectHashMap;
import io.netty.util.collection.LongObjectMap;
import io.netty.util.concurrent.Future;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.NoRouteToHostException;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.AlreadyConnectedException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ConnectionPendingException;
import java.nio.channels.UnresolvedAddressException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ObjLongConsumer;

import static io.netty.channel.unix.UnixChannelUtil.computeRemoteAddr;

abstract class AbstractIOUringChannel extends AbstractChannel implements UnixChannel {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getInstance(AbstractIOUringChannel.class);
    private static final int MAX_READ_AHEAD_PACKETS = 8;
    private static final AtomicInteger CONNECT_ID_COUNTER = new AtomicInteger();

    static final FutureContextListener<Buffer, Void> CLOSE_BUFFER = (b, f) -> SilentDispose.dispose(b, LOGGER);

    protected final LinuxSocket socket;
    protected final ObjectRing<Object> readsPending;
    protected final ObjectRing<Object> readsCompleted; // Either 'Failure', or a message (buffer, datagram, ...).
    protected final LongObjectMap<Object> cancelledReads;

    protected volatile boolean active;
    protected volatile SocketAddress local;
    protected volatile SocketAddress remote;

    protected SubmissionQueue submissionQueue;
    protected int currentCompletionResult;
    protected short currentCompletionData;

    private final Runnable pendingRead;
    private final Runnable rdHupRead;

    private short lastReadId;
    private boolean readPendingRegister;
    private boolean readPendingConnect;
    private ByteBuffer remoteAddressMem;
    private boolean scheduledRdHup;
    private boolean receivedRdHup;
    private boolean submittedClose;
    private boolean wasReadPendingAlready;
    private ChannelPromise delayedClose;

    /**
     * The future of the current connection attempt.  If not null, subsequent connection attempts will fail.
     */
    private ChannelPromise connectPromise;
    private ScheduledFuture<?> connectTimeoutFuture;
    private SocketAddress requestedRemoteAddress;
    private ByteBuffer remoteAddressMemory;
    private MsgHdrMemoryArray msgHdrMemoryArray;

    private short lastConnectId;

    protected AbstractIOUringChannel(final Channel parent, LinuxSocket socket, SocketAddress remote, boolean active) {
        super(parent);
        this.socket = socket;
        this.active = active;
        if (active) {
            // Directly cache local and remote addresses.
            local = socket.localAddress();
            this.remote = remote == null ? socket.remoteAddress() : remote;
        } else if (remote != null) {
            this.remote = remote;
        }
        pendingRead = this::submitReadForPending;
        rdHupRead = this::submitReadForRdHup;
        readsPending = new ObjectRing<>();
        readsCompleted = new ObjectRing<>();
        cancelledReads = new LongObjectHashMap<>(8);
    }

    @Override
    protected SocketAddress localAddress0() {
        return local;
    }

    @Override
    protected SocketAddress remoteAddress0() {
        return remote;
    }

    @Override
    protected void doBind(SocketAddress localAddress) throws Exception {
        if (local instanceof InetSocketAddress) {
            checkResolvable((InetSocketAddress) local);
        }
        socket.bind(localAddress); // Bind immediately, as AbstractChannel expects it to be done after this method call.
        if (fetchLocalAddress()) {
            local = socket.localAddress();
        } else {
            local = localAddress;
        }
        // ZTOFO :Fix me
        //cacheAddresses(local, remoteAddress());
    }

    protected static void checkResolvable(InetSocketAddress addr) {
        if (addr.isUnresolved()) {
            throw new UnresolvedAddressException();
        }
    }

    // TODO: Fix me
    protected final boolean fetchLocalAddress() {
        return socket.protocolFamily() != SocketProtocolFamily.UNIX;
    }

    @Override
    protected void doBeginRead() throws Exception {
        // Schedule a read operation. When completed, we'll get a callback to readComplete.
        if (submissionQueue == null) {
            readPendingRegister = true;
            return;
        }
        if (!wasReadPendingAlready) {
            wasReadPendingAlready = true;
            submitRead();
        }
    }

    private void submitRead() {
        // Submit reads until read handle says stop, we fill the submission queue, or hit max limit
        int maxPackets = Math.min(submissionQueue.remaining(), MAX_READ_AHEAD_PACKETS);
        int sumPackets = 0;
        int bufferSize = nextReadBufferSize();
        boolean morePackets = bufferSize > 0;

        while (morePackets) {
            ByteBuf readBuffer = alloc().directBuffer(bufferSize);
            assert readBuffer.isDirect();
            sumPackets++;
            morePackets = sumPackets < maxPackets && (bufferSize = nextReadBufferSize()) > 0;
            submissionQueue.link(morePackets);
            short readId = ++lastReadId;
            submitReadForReadBuffer(readBuffer, readId, sumPackets > 1, readsPending);
        }
    }

    private void submitNonBlockingRead() {
        assert readsPending.isEmpty();
        int bufferSize = nextReadBufferSize();
        if (bufferSize == 0) {
            return;
        }
        ByteBuf readBuffer = alloc().directBuffer(bufferSize);
        assert readBuffer.isDirect();
        assert readBuffer.hasMemoryAddress();
        short readId = ++lastReadId;
        submitReadForReadBuffer(readBuffer, readId, true, readsPending);
    }

    private void submitReadForPending() {
        if (active && readsPending.isEmpty()) {
            submitRead();
        }
    }

    private void submitReadForRdHup() {
        if (active && readsPending.isEmpty()) {
            submitNonBlockingRead();
        }
    }

    protected int nextReadBufferSize() {
        // TODO: Fix me
        return 16 * 1024; // readHandle().prepareRead();
    }

    protected void submitReadForReadBuffer(ByteBuf buffer, short readId, boolean nonBlocking,
                                             ObjLongConsumer<Object> pendingConsumer) {
        int flags = nonBlocking ? Native.MSG_DONTWAIT : 0;
        long udata = submissionQueue.addRecv(fd().intValue(),
                buffer.memoryAddress(), buffer.writerIndex(), buffer.writableBytes(), flags, readId);
        pendingConsumer.accept(buffer, udata);
    }

    protected void doClearScheduledRead() {
        // Using the lastReadId to differentiate our reads, means we avoid accidentally cancelling any future read.
        while (readsPending.poll()) {
            Object obj = readsPending.getPolledObject();
            long udata = readsPending.getPolledStamp();
            ReferenceCountUtil.touch(obj, "read cancelled");
            cancelledReads.put(udata, obj);
            submissionQueue.addCancel(fd().intValue(), udata);
        }
    }

    void readComplete(int res, long udata) {
        assert eventLoop().inEventLoop();
        if (res == Native.ERRNO_ECANCELED_NEGATIVE || res == Errors.ERRNO_EAGAIN_NEGATIVE) {
            Object obj = cancelledReads.remove(udata);
            if (obj == null) {
                obj = readsPending.remove(udata);
            }
            if (obj != null) {
                ReferenceCountUtil.safeRelease(obj);
                //SilentDispose.dispose(obj, logger());
            }
            return;
        }

        final Object obj;
        if (readsPending.hasNextStamp(udata) && readsPending.poll()) {
            obj = readsPending.getPolledObject();
        } else {
            // Out-of-order read completion? Weird. Should this ever happen?
            obj = readsPending.remove(udata);
        }
        if (obj != null) {
            if (res >= 0) {
                ReferenceCountUtil.touch(obj, "read completed");
                readsCompleted.push(prepareCompletedRead(obj, res), udata);
            } else {
                //SilentDispose.dispose(obj, logger());
                ReferenceCountUtil.safeRelease(obj);
                readsCompleted.push(new Failure(res), udata);
            }
        }
    }

    protected Object prepareCompletedRead(Object obj, int result) {
        ByteBuf buffer = (ByteBuf) obj;
        buffer.writerIndex(buffer.writerIndex() + result);
        return buffer;
    }

    void ioLoopCompleted() {
        if (!readsCompleted.isEmpty()) {
            readNow(); // Will call back into doReadNow.
        }
        WriteSink writeSink = this.writeSink;
        if (writeSink != null) {
            writeSink.complete(0, 0, 0, false);
            writeSink.writeLoopContinue();
            writeSink.writeLoopEnd();
            this.writeSink = null;
        }
    }

    @Override
    protected boolean doReadNow(ReadSink readSink) throws Exception {
        while (readsCompleted.poll()) {
            Object completion = readsCompleted.getPolledObject();
            if (completion instanceof Failure) {
                throw Errors.newIOException("channel.read", ((Failure) completion).result);
            }
            if (processRead(readSink, completion)) {
                // Leave it to the sub-class to decide if this buffer is EOF or not.
                return true;
            }
        }
        // We have no more completed reads. Stop the read loop.
        readSink.processRead(0, 0, null);
        return false;
    }


    /**
     * Process the given read.
     *
     * @return {@code true} if the channel should be closed, e.g. if a zero-readable buffer means EOF.
     */
    protected abstract boolean processRead(ReadSink readSink, Object read);

    @Override
    protected void readLoopComplete() {
        super.readLoopComplete();
        // If there are pending reads, or we received RDHUP (such that we want to drain inbound buffer),
        // then schedule a read to run later, after other tasks.
        // Those other tasks might issue their own reads, which we should not interferre with.
        // Another reason we need to schedule these to run later, is that the read-loop will cancel any unprocessed
        // reads after this method call.
        if (isReadPending()) {
            executor().execute(pendingRead);
        } else if (receivedRdHup) {
            executor().execute(rdHupRead);
        }
    }

    @Override
    protected void writeLoop(WriteSink writeSink) {
        this.writeSink = writeSink;
        try {
            writeSink.writeLoopStep();
        } catch (Throwable e) {
            handleWriteError(e);
        }
    }

    @Override
    protected void doWriteNow(WriteSink writeSink) {
        submitAllWriteMessages(writeSink);
        // We *MUST* submit all our messages, since we'll be releasing the outbound buffers after the doWriteNow call.
        submissionQueue.submit();
    }

    protected abstract void submitAllWriteMessages(WriteSink writeSink);

    abstract void writeComplete(int result, long udata);

    private abstract class IOUringUnsafe extends AbstractUnsafe {
        @Override
        public void connect(
                final SocketAddress remoteAddress, final SocketAddress localAddress, final ChannelPromise promise) {
            // Don't mark the connect promise as uncancellable as in fact we can cancel it as it is using
            // non-blocking io.
            if (promise.isDone() || !ensureOpen(promise)) {
                return;
            }

            if (delayedClose != null) {
                promise.tryFailure(annotateConnectException(new ClosedChannelException(), remoteAddress));
                return;
            }
            try {
                if (connectPromise != null) {
                    throw new ConnectionPendingException();
                }
                if (localAddress instanceof InetSocketAddress) {
                    checkResolvable((InetSocketAddress) localAddress);
                }

                if (remoteAddress instanceof InetSocketAddress) {
                    checkResolvable((InetSocketAddress) remoteAddress);
                }

                if (remote != null) {
                    // Check if already connected before trying to connect. This is needed as connect(...) will not return -1
                    // and set errno to EISCONN if a previous connect(...) attempt was setting errno to EINPROGRESS and finished
                    // later.
                    throw new AlreadyConnectedException();
                }

                if (localAddress != null) {
                    socket.bind(localAddress);
                }

                InetSocketAddress inetSocketAddress = (InetSocketAddress) remoteAddress;

                submitConnect(inetSocketAddress);
            } catch (Throwable t) {
                closeIfClosed();
                promise.tryFailure(annotateConnectException(t, remoteAddress));
                return;
            }
            connectPromise = promise;
            requestedRemoteAddress = remoteAddress;
            // Schedule connect timeout.
            int connectTimeoutMillis = config().getConnectTimeoutMillis();
            if (connectTimeoutMillis > 0) {
                connectTimeoutFuture = eventLoop().schedule(new Runnable() {
                    @Override
                    public void run() {
                        ChannelPromise connectPromise = AbstractIOUringChannel.this.connectPromise;
                        if (connectPromise != null && !connectPromise.isDone() &&
                                connectPromise.tryFailure(new ConnectTimeoutException(
                                        "connection timed out: " + remoteAddress))) {
                            close(voidPromise());
                        }
                    }
                }, connectTimeoutMillis, TimeUnit.MILLISECONDS);
            }

            promise.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) {
                    // If the connect future is cancelled we also cancel the timeout and close the
                    // underlying socket.
                    if (future.isCancelled()) {
                        cancelConnectTimeoutFuture();
                        connectPromise = null;
                        close(voidPromise());
                    }
                }
            });
        }


        private boolean isConnectPending() {
            return connectPromise != null;
        }

        /**
         * Should be called once the connect request is ready to be completed and {@link #isConnectPending()}
         * is {@code true}.
         * Calling this method if no {@link #isConnectPending() connect is pending} will result in an
         * {@link AlreadyConnectedException}.
         *
         * @return {@code true} if the connect operation completed, {@code false} otherwise.
         */
        protected final boolean finishConnect() {
            // Note this method is invoked by the event loop only if the connection attempt was
            // neither cancelled nor timed out.
            assert eventLoop().inEventLoop();

            if (!isConnectPending()) {
                throw new AlreadyConnectedException();
            }

            boolean connectStillInProgress = false;
            try {
                boolean wasActive = isActive();
                if (!doFinishConnect(requestedRemoteAddress)) {
                    connectStillInProgress = true;
                    return false;
                }
                requestedRemoteAddress = null;
                fulfillConnectPromise(connectPromise, wasActive);
            } catch (Throwable t) {
                fulfillConnectPromise(connectPromise, annotateConnectException(t, requestedRemoteAddress));
            } finally {
                if (!connectStillInProgress) {
                    // Check for null as the connectTimeoutFuture is only created if a connectTimeoutMillis > 0 is used
                    // See https://github.com/netty/netty/issues/1770
                    if (connectTimeoutFuture != null) {
                        connectTimeoutFuture.cancel(false);
                    }
                    connectPromise = null;
                }
            }
            return true;
        }

        private void fulfillConnectPromise(ChannelPromise promise, Throwable cause) {
            if (promise == null) {
                // Closed via cancellation and the promise has been notified already.
                return;
            }

            // Use tryFailure() instead of setFailure() to avoid the race against cancel().
            promise.tryFailure(cause);
            closeIfClosed();
        }

        private void fulfillConnectPromise(ChannelPromise promise, boolean wasActive) {
            if (promise == null) {
                // Closed via cancellation and the promise has been notified already.
                return;
            }
            active = true;

            if (local == null) {
                local = socket.localAddress();
            }
            computeRemote();

            // Register POLLRDHUP
            submitPollRdHup();

            // TODO: is the correct ?
            if (readPendingConnect) {
                submitRead();
                readPendingConnect = false;
            }

            // Get the state as trySuccess() may trigger an ChannelFutureListener that will close the
            // Channel.
            // We still need to ensure we call fireChannelActive() in this case.
            boolean active = isActive();

            // trySuccess() will return false if a user cancelled the connection attempt.
            boolean promiseSet = promise.trySuccess();

            // Regardless if the connection attempt was cancelled, channelActive() event should be triggered,
            // because what happened is what happened.
            if (!wasActive && active) {
                pipeline().fireChannelActive();
            }

            // If a user cancelled the connection attempt, close the channel, which is followed by channelInactive().
            if (!promiseSet) {
                close(voidPromise());
            }
        }

    }

    private void freeRemoteAddressMemory() {
        if (remoteAddressMemory != null) {
            Buffer.free(remoteAddressMemory);
            remoteAddressMemory = null;
        }
    }

    private void computeRemote() {
        if (requestedRemoteAddress instanceof InetSocketAddress) {
            remote = computeRemoteAddr((InetSocketAddress) requestedRemoteAddress, socket.remoteAddress());
        }
    }


    private void cancelConnectTimeoutFuture() {
        if (connectTimeoutFuture != null) {
            connectTimeoutFuture.cancel(false);
            connectTimeoutFuture = null;
        }
    }

    protected void submitConnect(InetSocketAddress remoteSocketAddr) {
        remoteAddressMemory = Buffer.allocateDirectWithNativeOrder(Native.SIZEOF_SOCKADDR_STORAGE);
        long remoteAddressMemoryAddress = Buffer.memoryAddress(remoteAddressMemory);
        SockaddrIn.write(socket.isIpv6(), remoteAddressMemoryAddress, remoteSocketAddr);
        lastConnectId = nextConnectId();
        submissionQueue.addConnect(fd().intValue(), remoteAddressMemoryAddress,
                Native.SIZEOF_SOCKADDR_STORAGE, lastConnectId);
    }

    @Override
    protected Object filterOutboundMessage(Object msg) {
        if (msg instanceof ByteBuf) {
            ByteBuf buf = (ByteBuf) msg;
            return UnixChannelUtil.isBufferCopyNeededForWrite(buf)? newDirectBuffer(buf) : buf;
        }

        throw new UnsupportedOperationException("unsupported message type");
    }

    private static short nextConnectId() {
        // Compute the next connect id, but skip 0, because that's the default value for lastConnectId.
        short result;
        do {
            result = (short) CONNECT_ID_COUNTER.incrementAndGet();
        } while (result == 0);
        return result;
    }

    void connectComplete(int res, long udata) {
        short connectId = UserData.decodeData(udata);
        if (connectId != lastConnectId) {
            // This can happen with file descriptor reuse, where the connect completion was for a now-closed channel.
            logger().debug("Ignoring connect completion for unrecognized connect call id: " +
                            "{} for fd {} (last connect id: {})",
                    connectId, fd().intValue(), lastConnectId);
            return;
        }
        currentCompletionResult = res;
        freeRemoteAddressMemory();

        ((IOUringUnsafe) unsafe()).finishConnect();
    }

    protected boolean doFinishConnect(SocketAddress requestedRemoteAddress) throws Exception {
        int res = currentCompletionResult;
        currentCompletionResult = 0;
        currentCompletionData = 0;
        if (res < 0) {
            IOException nativeException = Errors.newIOException("connect", res);
            if (res == Errors.ERROR_ECONNREFUSED_NEGATIVE) {
                ConnectException refused = new ConnectException(nativeException.getMessage());
                refused.initCause(nativeException);
                throw refused;
            }
            if (res == Errors.ERROR_EHOSTUNREACH_NEGATIVE) { // EHOSTUNREACH: No route to host
                NoRouteToHostException unreach = new NoRouteToHostException(nativeException.getMessage());
                unreach.initCause(nativeException);
                throw unreach;
            }
            SocketException exception = new SocketException(nativeException.getMessage());
            exception.initCause(nativeException);
            throw exception;
        }
        if (fetchLocalAddress()) {
            local = socket.localAddress();
        }
        if (socket.finishConnect()) {
            active = true;
            if (requestedRemoteAddress instanceof InetSocketAddress) {
                remote = computeRemoteAddr((InetSocketAddress) requestedRemoteAddress,
                        (InetSocketAddress) socket.remoteAddress());
            } else {
                remote = requestedRemoteAddress;
            }
            submitPollRdHup();
            if (readPendingConnect) {
                submitRead();
                readPendingConnect = false;
            }
            return true;
        }
        return false;
    }

    private void submitPollRdHup() {
        submissionQueue.addPollRdHup(fd().intValue());
        scheduledRdHup = true;
    }

    void completeRdHup(int res) {
        if (res == Native.ERRNO_ECANCELED_NEGATIVE) {
            return;
        }
        receivedRdHup = true;
        scheduledRdHup = false;
        if (active && readsPending.isEmpty()) {
            // Schedule a read to drain inbound buffer and notice the EOF.
            submitNonBlockingRead();
        }
    }

    void completeChannelRegister(SubmissionQueue submissionQueue) {
        this.submissionQueue = submissionQueue;
        if (active) {
            submitPollRdHup();
        }
        if (readPendingRegister) {
            readPendingRegister = false;
            read();
        }
    }

    @Override
    protected void doDisconnect() throws Exception {
        active = false;
    }

    @Override
    protected Future<Executor> prepareToClose() {
        // Prevent more operations from being submitted.
        active = false;
        // Cancel any pending connect.
        if (isConnectPending()) {
            submissionQueue.addCancel(fd().intValue(),
                    UserData.encode(fd().intValue(), Native.IORING_OP_CONNECT, lastConnectId));
        }
        // Cancel all pending reads.
        doClearScheduledRead();
        // Cancel any RDHUP poll
        if (scheduledRdHup) {
            submissionQueue.addPollRemove(fd().intValue(), Native.POLLRDHUP);
        }
        closeTransportNow();
        return prepareClosePromise.asFuture();
    }

    @Override
    protected void doClose() {
        tryDisposeAll(readsPending);
        tryDisposeAll(readsCompleted);
        freeRemoteAddressMemory();
    }

    private void tryDisposeAll(ObjectRing<?> ring) {
        while (ring.poll()) {
            SilentDispose.trySilentDispose(ring.getPolledObject(), logger());
        }
    }

    void closeTransportNow() {
        if (!submittedClose) {
            submissionQueue.addClose(socket.intValue(), false, (short) 0);
            submittedClose = true;
        } else {
            logger().warn("Double-close attempted for {}", this);
        }
    }

    void closeComplete(int res, long udata) {
        if (socket.markClosed()) {
            delayedClose.setSuccess();
        }
    }

    @Override
    public FileDescriptor fd() {
        return socket;
    }

    @Override
    public boolean isOpen() {
        return socket.isOpen();
    }

    @Override
    public boolean isActive() {
        return active;
    }


    protected final ByteBuf newDirectBuffer(ByteBuf buf) {
        return newDirectBuffer(buf, buf);
    }

    protected final ByteBuf newDirectBuffer(Object holder, ByteBuf buf) {
        final int readableBytes = buf.readableBytes();
        if (readableBytes == 0) {
            ReferenceCountUtil.release(holder);
            return Unpooled.EMPTY_BUFFER;
        }

        final ByteBufAllocator alloc = alloc();
        if (alloc.isDirectBufferPooled()) {
            return newDirectBuffer0(holder, buf, alloc, readableBytes);
        }

        final ByteBuf directBuf = ByteBufUtil.threadLocalDirectBuffer();
        if (directBuf == null) {
            return newDirectBuffer0(holder, buf, alloc, readableBytes);
        }

        directBuf.writeBytes(buf, buf.readerIndex(), readableBytes);
        ReferenceCountUtil.safeRelease(holder);
        return directBuf;
    }

    private static ByteBuf newDirectBuffer0(Object holder, ByteBuf buf, ByteBufAllocator alloc, int capacity) {
        final ByteBuf directBuf = alloc.directBuffer(capacity);
        directBuf.writeBytes(buf, buf.readerIndex(), capacity);
        ReferenceCountUtil.safeRelease(holder);
        return directBuf;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(fd: " + socket.intValue() + ')' + super.toString();
    }

    protected abstract InternalLogger logger();


    boolean isIpv6() {
        return socket.isIpv6();
    }

    private static final class Failure {
        final int result;

        private Failure(int result) {
            this.result = result;
        }
    }
}
