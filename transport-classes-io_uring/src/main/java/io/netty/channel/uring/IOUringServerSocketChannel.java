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

import io.netty.channel.Channel;
import io.netty.channel.socket.ServerSocketChannel;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

public final class IOUringServerSocketChannel extends AbstractIOUringServerChannel implements ServerSocketChannel {
    private final IOUringServerSocketChannelConfig config;

    public IOUringServerSocketChannel() {
        // We don't use a blocking fd for the server channel at the moment as
        // there is no support for IORING_CQE_F_SOCK_NONEMPTY and IORING_ACCEPT_DONTWAIT
        // at the moment. Once these land in the kernel we should check if we can use these and if so make
        // the fd blocking to get rid of POLLIN etc.
        // See:
        //
        //  - https://lore.kernel.org/netdev/20240509180627.204155-1-axboe@kernel.dk/
        //  - https://lore.kernel.org/io-uring/20240508142725.91273-1-axboe@kernel.dk/
        super(LinuxSocket.newSocketStream(), false);
        this.config = new IOUringServerSocketChannelConfig(this);
    }

    @Override
    public IOUringServerSocketChannelConfig config() {
        return config;
    }

    @Override
    Channel newChildChannel(int fd, long acceptedAddressMemoryAddress, long acceptedAddressLengthMemoryAddress) {
        final InetSocketAddress address;
        if (socket.isIpv6()) {
            byte[] ipv6Array = registration().ioHandler().inet6AddressArray();
            byte[] ipv4Array = registration().ioHandler().inet4AddressArray();
            address = SockaddrIn.readIPv6(acceptedAddressMemoryAddress, ipv6Array, ipv4Array);
        } else {
            byte[] addressArray = registration().ioHandler().inet4AddressArray();
            address = SockaddrIn.readIPv4(acceptedAddressMemoryAddress, addressArray);
        }
        return new IOUringSocketChannel(this, new LinuxSocket(fd), address);
    }

    @Override
    public InetSocketAddress remoteAddress() {
        return (InetSocketAddress) super.remoteAddress();
    }

    @Override
    public InetSocketAddress localAddress() {
        return (InetSocketAddress) super.localAddress();
    }

    @Override
    public void doBind(SocketAddress localAddress) throws Exception {
        super.doBind(localAddress);
        if (IOUring.isTcpFastOpenServerSideAvailable()) {
            int fastOpen = config().getTcpFastopen();
            if (fastOpen > 0) {
                socket.setTcpFastOpen(fastOpen);
            }
        }
        socket.listen(config.getBacklog());
        active = true;
    }
}
