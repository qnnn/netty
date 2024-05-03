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

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.unix.SegmentedDatagramPacket;
import io.netty.testsuite.transport.TestsuitePermutation;
import io.netty.testsuite.transport.socket.DatagramUnicastInetTest;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class IOUringDatagramUnicastTest extends DatagramUnicastInetTest {

    @BeforeAll
    public static void loadJNI() {
        assumeTrue(IOUring.isAvailable());
    }

    @Override
    protected List<TestsuitePermutation.BootstrapComboFactory<Bootstrap, Bootstrap>> newFactories() {
        return IOUringSocketTestPermutation.INSTANCE.datagram(SocketProtocolFamily.INET);
    }

    @Test
    @Timeout(8)
    public void testRecvMsgDontBlock(TestInfo testInfo) throws Throwable {
        run(testInfo, this::testRecvMsgDontBlock);
    }

    public void testRecvMsgDontBlock(Bootstrap sb, Bootstrap cb) throws Throwable {
        Channel sc = null;
        Channel cc = null;

        try {
            cb.handler(new SimpleChannelInboundHandler<Object>() {
                @Override
                protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                    // NOOP.
                }
            });
            cc = cb.bind(newSocketAddress()).sync().channel();

            CountDownLatch readLatch = new CountDownLatch(1);
            CountDownLatch readCompleteLatch = new CountDownLatch(1);
            sc = sb.handler(new ChannelInboundHandlerAdapter() {
                @Override
                public void channelRead(ChannelHandlerContext ctx, Object msg) {
                    readLatch.countDown();
                    ReferenceCountUtil.release(msg);
                }

                @Override
                public void channelReadComplete(ChannelHandlerContext ctx) {
                    readCompleteLatch.countDown();
                }
            }).option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(2048))
                    .bind(newSocketAddress()).sync().channel();
            InetSocketAddress addr = sendToAddress((InetSocketAddress) sc.localAddress());
            cc.writeAndFlush(new DatagramPacket(cc.alloc().buffer().writeZero(512),  addr)).sync();

            readLatch.await();
            readCompleteLatch.await();
        } finally {
            if (cc != null) {
                cc.close().sync();
            }
            if (sc != null) {
                sc.close().sync();
            }
        }
    }

    @Test
    public void testSendSegmentedDatagramPacket(TestInfo testInfo) throws Throwable {
        run(testInfo, this::testSendSegmentedDatagramPacket);
    }

    public void testSendSegmentedDatagramPacket(Bootstrap sb, Bootstrap cb) throws Throwable {
        testSegmentedDatagramPacket(sb, cb, false, false);
    }

    @Disabled("Loopback does not support packet segmentation.")
    @Test
    public void testSendSegmentedDatagramPacketComposite(TestInfo testInfo) throws Throwable {
        run(testInfo, this::testSendSegmentedDatagramPacketComposite);
    }

    public void testSendSegmentedDatagramPacketComposite(Bootstrap sb, Bootstrap cb) throws Throwable {
        testSegmentedDatagramPacket(sb, cb, true, false);
    }

    @Disabled("Loopback does not support packet segmentation.")
    @Test
    public void testSendAndReceiveSegmentedDatagramPacket(TestInfo testInfo) throws Throwable {
        run(testInfo, this::testSendAndReceiveSegmentedDatagramPacket);
    }

    public void testSendAndReceiveSegmentedDatagramPacket(Bootstrap sb, Bootstrap cb) throws Throwable {
        testSegmentedDatagramPacket(sb, cb, false, true);
    }

    @Disabled("Loopback does not support packet segmentation.")
    @Test
    public void testSendAndReceiveSegmentedDatagramPacketComposite(TestInfo testInfo) throws Throwable {
        run(testInfo, this::testSendAndReceiveSegmentedDatagramPacketComposite);
    }

    public void testSendAndReceiveSegmentedDatagramPacketComposite(Bootstrap sb, Bootstrap cb) throws Throwable {
        testSegmentedDatagramPacket(sb, cb, true, true);
    }

    private void testSegmentedDatagramPacket(Bootstrap sb, Bootstrap cb, boolean composite, boolean gro)
            throws Throwable {
        assumeTrue(IOUringDatagramChannel.isSegmentedDatagramPacketSupported());
        Channel sc = null;
        Channel cc = null;

        try {
            cb.handler(new SimpleChannelInboundHandler<Object>() {
                @Override
                public void channelRead0(ChannelHandlerContext ctx, Object msgs) {
                    // Nothing will be sent.
                }
            });

            cc = cb.bind(newSocketAddress()).sync().channel();
            if (!(cc instanceof IOUringDatagramChannel)) {
                // Only supported for the native io_uring transport.
                return;
            }
            final int numBuffers = 16;
            final int segmentSize = 512;
            int bufferCapacity = numBuffers * segmentSize;
            final CountDownLatch latch = new CountDownLatch(numBuffers);
            AtomicReference<Throwable> errorRef = new AtomicReference<>();
            if (gro) {
                // Enable GRO and also ensure we can read everything with one read as otherwise
                // we will drop things on the floor.
                sb.option(IOUringChannelOption.UDP_GRO, true);
                sb.option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(bufferCapacity));
            }
            sc = sb.handler(new SimpleChannelInboundHandler<Object>() {
                @Override
                public void channelRead0(ChannelHandlerContext ctx, Object msg) {
                    if (msg instanceof DatagramPacket) {
                        DatagramPacket packet = (DatagramPacket) msg;
                        int packetSize = packet.content().readableBytes();
                        assertEquals(segmentSize, packetSize, "Unexpected datagram packet size");
                        latch.countDown();
                    } else {
                        fail("Unexpected message of type " + msg.getClass() + ": " + msg);
                    }
                }

                @Override
                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                    do {
                        Throwable throwable = errorRef.get();
                        if (throwable != null) {
                            if (throwable != cause) {
                                throwable.addSuppressed(cause);
                            }
                            break;
                        }
                    } while (!errorRef.compareAndSet(null, cause));
                    super.exceptionCaught(ctx, cause);
                }
            }).bind(newSocketAddress()).sync().channel();

            if (gro && !(sc instanceof IOUringDatagramChannel)) {
                // Only supported for the native io_uring transport.
                return;
            }
            if (sc instanceof IOUringDatagramChannel) {
                assertEquals(gro, sc.config().getOption(IOUringChannelOption.UDP_GRO));
            }
            InetSocketAddress addr = sendToAddress((InetSocketAddress) sc.localAddress());
            final ByteBuf buffer;
            if (composite) {
                ByteBuf[] components = new ByteBuf[numBuffers];
                for (int i = 0; i < numBuffers; i++) {
                    components[i] = cc.alloc().buffer(segmentSize);
                    components[i].writeZero(segmentSize);
                }
                buffer = Unpooled.wrappedBuffer(components);
            } else {
                buffer = cc.alloc().buffer(bufferCapacity);
                buffer.writeZero(segmentSize);
            }
            cc.writeAndFlush(new SegmentedDatagramPacket(buffer, segmentSize, addr)).sync();

            if (!latch.await(10, TimeUnit.SECONDS)) {
                Throwable error = errorRef.get();
                if (error != null) {
                    throw error;
                }
                fail();
            }
        } finally {
            if (cc != null) {
                cc.close().sync();
            }
            if (sc != null) {
                sc.close().sync();
            }
        }
    }

    @Override
    protected boolean supportDisconnect() {
        return false;
    }
}
