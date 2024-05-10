/*
 * Copyright 2020 The Netty Project
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
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.socket.DuplexChannel;
import io.netty.channel.unix.IovArray;
import io.netty.channel.unix.Limits;
import io.netty.util.internal.UnstableApi;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.net.SocketAddress;
import java.io.IOException;
import java.util.concurrent.Executor;

import static io.netty.channel.unix.Errors.ioResult;

abstract class AbstractIOUringStreamChannel extends AbstractIOUringChannel implements DuplexChannel {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(AbstractIOUringStreamChannel.class);

    // Store the opCode so we know if we used WRITE or WRITEV.
    private byte writeOpCode;

    // Keep track of the ids used for write and read so we can cancel these when needed.
    private long writeId;
    private long readId;

    AbstractIOUringStreamChannel(Channel parent, LinuxSocket socket, boolean active) {
        // Use a blocking fd, we can make use of fastpoll.
        super(parent, LinuxSocket.makeBlocking(socket), active);
    }

    AbstractIOUringStreamChannel(Channel parent, LinuxSocket socket, SocketAddress remote) {
        // Use a blocking fd, we can make use of fastpoll.
        super(parent, LinuxSocket.makeBlocking(socket), remote);
    }

    @Override
    protected AbstractUringUnsafe newUnsafe() {
        return new IOUringStreamUnsafe();
    }

    @Override
    public ChannelFuture shutdown() {
        return shutdown(newPromise());
    }

    @Override
    public ChannelFuture shutdown(final ChannelPromise promise) {
        ChannelFuture shutdownOutputFuture = shutdownOutput();
        if (shutdownOutputFuture.isDone()) {
            shutdownOutputDone(shutdownOutputFuture, promise);
        } else {
            shutdownOutputFuture.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(final ChannelFuture shutdownOutputFuture) throws Exception {
                    shutdownOutputDone(shutdownOutputFuture, promise);
                }
            });
        }
        return promise;
    }

    @UnstableApi
    @Override
    protected final void doShutdownOutput() throws Exception {
        socket.shutdown(false, true);
    }

    private void shutdownInput0(final ChannelPromise promise) {
        try {
            socket.shutdown(true, false);
            promise.setSuccess();
        } catch (Throwable cause) {
            promise.setFailure(cause);
        }
    }

    @Override
    public boolean isOutputShutdown() {
        return socket.isOutputShutdown();
    }

    @Override
    public boolean isInputShutdown() {
        return socket.isInputShutdown();
    }

    @Override
    public boolean isShutdown() {
        return socket.isShutdown();
    }

    @Override
    public ChannelFuture shutdownOutput() {
        return shutdownOutput(newPromise());
    }

    @Override
    public ChannelFuture shutdownOutput(final ChannelPromise promise) {
        EventLoop loop = eventLoop();
        if (loop.inEventLoop()) {
            ((AbstractUnsafe) unsafe()).shutdownOutput(promise);
        } else {
            loop.execute(new Runnable() {
                @Override
                public void run() {
                    ((AbstractUnsafe) unsafe()).shutdownOutput(promise);
                }
            });
        }

        return promise;
    }

    @Override
    public ChannelFuture shutdownInput() {
        return shutdownInput(newPromise());
    }

    @Override
    public ChannelFuture shutdownInput(final ChannelPromise promise) {
        Executor closeExecutor = ((IOUringStreamUnsafe) unsafe()).prepareToClose();
        if (closeExecutor != null) {
            closeExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    shutdownInput0(promise);
                }
            });
        } else {
            EventLoop loop = eventLoop();
            if (loop.inEventLoop()) {
                shutdownInput0(promise);
            } else {
                loop.execute(new Runnable() {
                    @Override
                    public void run() {
                        shutdownInput0(promise);
                    }
                });
            }
        }
        return promise;
    }

    private void shutdownOutputDone(final ChannelFuture shutdownOutputFuture, final ChannelPromise promise) {
        ChannelFuture shutdownInputFuture = shutdownInput();
        if (shutdownInputFuture.isDone()) {
            shutdownDone(shutdownOutputFuture, shutdownInputFuture, promise);
        } else {
            shutdownInputFuture.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture shutdownInputFuture) throws Exception {
                    shutdownDone(shutdownOutputFuture, shutdownInputFuture, promise);
                }
            });
        }
    }

    private static void shutdownDone(ChannelFuture shutdownOutputFuture,
                                     ChannelFuture shutdownInputFuture,
                                     ChannelPromise promise) {
        Throwable shutdownOutputCause = shutdownOutputFuture.cause();
        Throwable shutdownInputCause = shutdownInputFuture.cause();
        if (shutdownOutputCause != null) {
            if (shutdownInputCause != null) {
                logger.info("Exception suppressed because a previous exception occurred.",
                             shutdownInputCause);
            }
            promise.setFailure(shutdownOutputCause);
        } else if (shutdownInputCause != null) {
            promise.setFailure(shutdownInputCause);
        } else {
            promise.setSuccess();
        }
    }

    @Override
    protected void doRegister(ChannelPromise promise) {
        super.doRegister(promise);
        promise.addListener(f -> {
            if (f.isSuccess()) {
                if (active) {
                    // Register for POLLRDHUP if this channel is already considered active.
                    schedulePollRdHup();
                }
            }
        });
    }

    private final class IOUringStreamUnsafe extends AbstractUringUnsafe {

        // Overridden here just to be able to access this method from AbstractEpollStreamChannel
        @Override
        protected Executor prepareToClose() {
            return super.prepareToClose();
        }

        private ByteBuf readBuffer;
        private IovArray iovArray;

        @Override
        protected int scheduleWriteMultiple(ChannelOutboundBuffer in) {
            assert iovArray == null;
            assert writeId == 0;
            int numElements = Math.min(in.size(), Limits.IOV_MAX);
            ByteBuf iovArrayBuffer = alloc().directBuffer(numElements * IovArray.IOV_SIZE);
            iovArray = new IovArray(iovArrayBuffer);
            try {
                int offset = iovArray.count();
                in.forEachFlushedMessage(iovArray);

                IOUringIoRegistration registration = registration();
                IOUringIoOps ops = IOUringIoOps.newWritev(fd().intValue(), 0, 0, iovArray.memoryAddress(offset),
                        iovArray.count() - offset, registration.nextOpsId());
                long id = ops.udata();
                byte opCode = ops.opcode();
                registration.submit(ops);
                writeId = id;
                writeOpCode = opCode;
            } catch (Exception e) {
                iovArray.release();
                iovArray = null;

                // This should never happen, anyway fallback to single write.
                scheduleWriteSingle(in.current());
            }
            return 1;
        }

        @Override
        protected int scheduleWriteSingle(Object msg) {
            assert iovArray == null;
            assert writeId == 0;
            ByteBuf buf = (ByteBuf) msg;

            IOUringIoRegistration registration = registration();
            IOUringIoOps ops = IOUringIoOps.newWrite(fd().intValue(), 0, 0,
                    buf.memoryAddress() + buf.readerIndex(), buf.readableBytes(), registration.nextOpsId());
            long id = ops.udata();
            byte opCode = ops.opcode();
            registration.submit(ops);
            writeId = id;
            writeOpCode = opCode;
            registration.submit(ops);
            return 1;
        }

        @Override
        protected int scheduleRead0(boolean first) {
            assert readBuffer == null;
            assert readId == 0;

            final IOUringRecvByteAllocatorHandle allocHandle = recvBufAllocHandle();
            ByteBuf byteBuf = allocHandle.allocate(alloc());
            allocHandle.attemptedBytesRead(byteBuf.writableBytes());

            readBuffer = byteBuf;

            IOUringIoRegistration registration = registration();
            // Depending on if this is the first read or not we will use Native.MSG_DONTWAIT.
            // The idea is that if the socket is blocking we can do the first read in a blocking fashion
            // and so not need to also register POLLIN. As we can not 100 % sure if reads after the first will
            // be possible directly we schedule these with Native.MSG_DONTWAIT. This allows us to still be
            // able to signal the fireChannelReadComplete() in a timely manner and be consistent with other
            // transports.
            IOUringIoOps ops = IOUringIoOps.newRecv(fd().intValue(), 0, first ? 0 : Native.MSG_DONTWAIT,
                    byteBuf.memoryAddress() + byteBuf.writerIndex(), byteBuf.writableBytes(), registration.nextOpsId());
            long id = ops.udata();
            registration.submit(ops);
            readId = id;
            return 1;
        }

        @Override
        protected void readComplete0(int res, int flags, int data, int outstanding) {
            assert readId != 0;
            readId = 0;
            boolean close = false;

            final IOUringRecvByteAllocatorHandle allocHandle = recvBufAllocHandle();
            final ChannelPipeline pipeline = pipeline();
            ByteBuf byteBuf = this.readBuffer;
            this.readBuffer = null;
            assert byteBuf != null;

            try {
                if (res < 0) {
                    if (res == Native.ERRNO_ECANCELED_NEGATIVE) {
                        byteBuf.release();
                        return;
                    }
                    // If res is negative we should pass it to ioResult(...) which will either throw
                    // or convert it to 0 if we could not read because the socket was not readable.
                    allocHandle.lastBytesRead(ioResult("io_uring read", res));
                } else if (res > 0) {
                    byteBuf.writerIndex(byteBuf.writerIndex() + res);
                    allocHandle.lastBytesRead(res);
                } else {
                    // EOF which we signal with -1.
                    allocHandle.lastBytesRead(-1);
                }
                if (allocHandle.lastBytesRead() <= 0) {
                    // nothing was read, release the buffer.
                    byteBuf.release();
                    byteBuf = null;
                    close = allocHandle.lastBytesRead() < 0;
                    if (close) {
                        // There is nothing left to read as we received an EOF.
                        shutdownInput(false);
                    }
                    allocHandle.readComplete();
                    pipeline.fireChannelReadComplete();
                    return;
                }

                allocHandle.incMessagesRead(1);
                pipeline.fireChannelRead(byteBuf);
                byteBuf = null;
                if (allocHandle.continueReading() &&
                        // If IORING_CQE_F_SOCK_NONEMPTY is supported we should check for it first before
                        // trying to schedule a read. If it's supported and not part of the flags we know for sure
                        // that the next read (which would be using Native.MSG_DONTWAIT) will complete without
                        // be able to read any data. This is useless work and we can skip it.
                        (!IOUring.isIOUringCqeFSockNonEmptySupported() ||
                        (flags & Native.IORING_CQE_F_SOCK_NONEMPTY) != 0)) {
                    // Let's schedule another read.
                    scheduleRead(false);
                } else {
                    // We did not fill the whole ByteBuf so we should break the "read loop" and try again later.
                    allocHandle.readComplete();
                    pipeline.fireChannelReadComplete();
                }
            } catch (Throwable t) {
                handleReadException(pipeline, byteBuf, t, close, allocHandle);
            }
        }

        private void handleReadException(ChannelPipeline pipeline, ByteBuf byteBuf,
                                         Throwable cause, boolean close,
                                         IOUringRecvByteAllocatorHandle allocHandle) {
            if (byteBuf != null) {
                if (byteBuf.isReadable()) {
                    pipeline.fireChannelRead(byteBuf);
                } else {
                    byteBuf.release();
                }
            }
            allocHandle.readComplete();
            pipeline.fireChannelReadComplete();
            pipeline.fireExceptionCaught(cause);
            if (close || cause instanceof IOException) {
                shutdownInput(false);
            }
        }

        @Override
        boolean writeComplete0(int res, int flags, int data, int outstanding) {
            assert writeId != 0;
            writeId = 0;
            writeOpCode = 0;
            IovArray iovArray = this.iovArray;
            if (iovArray != null) {
                this.iovArray = null;
                iovArray.release();
            }
            if (res >= 0) {
                unsafe().outboundBuffer().removeBytes(res);
            } else if (res == Native.ERRNO_ECANCELED_NEGATIVE) {
                return true;
            } else {
                try {
                    if (ioResult("io_uring write", res) == 0) {
                        return false;
                    }
                } catch (Throwable cause) {
                    handleWriteError(cause);
                }
            }
            return true;
        }
    }

    @Override
    protected void cancelOutstandingReads(IOUringIoRegistration registration, int numOutstandingReads) {
        if (readId != 0) {
            // Let's try to cancel outstanding reads as these might be submitted and waiting for data (via fastpoll).
            assert numOutstandingReads == 1;
            IOUringIoOps ops = IOUringIoOps.newAsyncCancel(fd().intValue(), 0, readId, Native.IORING_OP_READV);
            registration.submit(ops);
        } else {
            assert numOutstandingReads == 0;
        }
    }

    @Override
    protected void cancelOutstandingWrites(IOUringIoRegistration registration,  int numOutstandingWrites) {
        if (writeId != 0) {
            // Let's try to cancel outstanding writes as these might be submitted and waiting to finish writing
            // (via fastpoll).
            assert numOutstandingWrites == 1;
            assert writeOpCode != 0;
            registration.submit(IOUringIoOps.newAsyncCancel(fd().intValue(), 0, writeId, writeOpCode));
        } else {
            assert numOutstandingWrites == 0;
        }
    }
}
