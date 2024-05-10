/*
 * Copyright 2024 The Netty Project
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

import io.netty.channel.IoOps;

/**
 * {@link IoOps} for
 * <a href="https://github.com/axboe/liburing/blob/liburing-2.6/src/include/liburing/io_uring.h">IO_uring</a>.
 */
public final class IOUringIoOps implements IoOps {

    private final byte opcode;
    private final int flags;
    private final short ioPrio;
    private final int fd;
    private final int rwFlags;
    private final long bufferAddress;
    private final int length;
    private final long offset;
    private final long udata;

    /**
     * Create a new instance
     *
     * @param opcode        the operation.
     * @param flags         the flags
     * @param ioPrio        the priority.
     * @param fd            the filedescriptor.
     * @param rwFlags       the flags specific for the op.
     * @param bufferAddress the bufferaddress
     * @param length        the length
     * @param offset        the offset.
     * @param data         the user data that will be passed back on completion.
     */
    public IOUringIoOps(byte opcode, int flags, short ioPrio, int fd, int rwFlags, long bufferAddress,
                        int length, long offset, short data) {
        this.opcode = opcode;
        this.flags = flags;
        this.ioPrio = ioPrio;
        this.fd = fd;
        this.rwFlags = rwFlags;
        this.bufferAddress = bufferAddress;
        this.length = length;
        this.offset = offset;
        // We encode the fd, op and udata into a long so we are able to use it later.
        this.udata = UserData.encode(fd, opcode, data);
    }

    /**
     * Returns the filedescriptor.
     *
     * @return  fd
     */
    public int fd() {
        return fd;
    }

    /**
     * Returns the opcode.
     *
     * @return  opcode
     */
    public byte opcode() {
        return opcode;
    }

    /**
     * Returns the flags that will be applied.
     *
     * @return  flags
     */
    public int flags() {
        return flags;
    }

    /**
     * Returns the priority.
     *
     * @return ioPrio
     */
    public short ioPrio() {
        return ioPrio;
    }

    /**
     * Returns the rwFlags that will be applied. These are specific to the opcode.
     *
     * @return  rwFlags
     */
    public int rwFlags() {
        return rwFlags;
    }

    /**
     * Returns the bufferAddress that will be used. This is specific to the opcode.
     *
     * @return  bufferAddress
     */
    public long bufferAddress() {
        return bufferAddress;
    }

    /**
     * Returns the length that will be used. This is specific to the opcode.
     *
     * @return  length
     */
    public int length() {
        return length;
    }

    /**
     * Returns the offset that will be used. This is specific to the opcode.
     *
     * @return  offset
     */
    public long offset() {
        return offset;
    }

    /**
     * Returns the data that the user attached to the op. This data will be passed back on completion.
     *
     * @return  data
     */
    public short data() {
        return UserData.decodeData(udata);
    }

    /**
     * Return the full udata that is a combination of {@link #fd()}, {@link #opcode()} and {@link #data()}.
     *
     * @return  udata.
     */
    public long udata() {
        return udata;
    }

    @Override
    public String toString() {
        return "IOUringIoOps{" +
                "opcode=" + opcode +
                ", flags=" + flags +
                ", ioPrio=" + ioPrio +
                ", fd=" + fd +
                ", rwFlags=" + rwFlags +
                ", bufferAddress=" + bufferAddress +
                ", length=" + length +
                ", offset=" + offset +
                ", udata=" + udata +
                '}';
    }

    /**
     * Returns a new {@code OP_ASYNC_CANCEL} {@link IOUringIoOps}.
     *
     * @param fd        the filedescriptor
     * @param flags     the flags.
     * @param udata     the {@link #udata()} of the previous {@link IOUringIoOps} that should be cancelled.
     * @param data      the data
     * @return          ops.
     */
    public static IOUringIoOps newAsyncCancel(int fd, int flags, long udata, short data) {
        // Best effort to cancel the
        return new IOUringIoOps(Native.IORING_OP_ASYNC_CANCEL, flags, (short) 0, fd, 0,
                udata, 0, 0, data);
    }

    /**
     * Returns a new {@code OP_CLOSE} {@link IOUringIoOps}.
     *
     * @param fd        the filedescriptor
     * @param flags     the flags.
     * @param data      the data
     * @return          ops.
     */
    public static IOUringIoOps newClose(int fd, int flags, short data) {
        return new IOUringIoOps(Native.IORING_OP_CLOSE, flags, (short) 0, fd, 0, 0, 0, 0, data);
    }

    /**
     * Returns a new {@code OP_POLL_ADD} {@link IOUringIoOps}.
     *
     * @param fd        the filedescriptor
     * @param flags     the flags.
     * @param mask      the mask.
     * @param data      the data
     * @return          ops.
     */
    public static IOUringIoOps newPollAdd(int fd, int flags, int mask, short data) {
        return new IOUringIoOps(Native.IORING_OP_POLL_ADD, flags, (short) 0, fd, mask, 0, 0, 0, data);
    }

    /**
     * Returns a new {@code OP_SENDMSG} {@link IOUringIoOps}.
     *
     * @param fd        the filedescriptor
     * @param flags     the flags.
     * @param msgFlags  the msg flags.
     * @param data      the data
     * @return          ops.
     */
    public static IOUringIoOps newSendmsg(int fd, int flags, int msgFlags, long address, short data) {
        return new IOUringIoOps(Native.IORING_OP_SENDMSG, flags, (short) 0, fd, msgFlags, address, 1, 0, data);
    }

    /**
     * Returns a new {@code OP_CONNECT} {@link IOUringIoOps}.
     *
     * @param fd                    the filedescriptor
     * @param flags                 the flags.
     * @param remoteMemoryAddress   the memory address of the sockaddr_storage.
     * @param data                  the data
     * @return                      ops.
     */
    public static IOUringIoOps newConnect(int fd, int flags, long remoteMemoryAddress, short data) {
        return new IOUringIoOps(Native.IORING_OP_CONNECT, flags, (short) 0, fd, 0, remoteMemoryAddress,
                0, Native.SIZEOF_SOCKADDR_STORAGE, data);
    }

    /**
     * Returns a new {@code OP_POLL_REMOVE} {@link IOUringIoOps}.
     *
     * @param fd        the filedescriptor
     * @param flags     the flags.
     * @param udata     the {@link #udata()} that was used to add before.
     * @param data      the data
     * @return          ops.
     */
    public static IOUringIoOps newPollRemove(int fd, int flags, long udata, short data) {
        return new IOUringIoOps(Native.IORING_OP_POLL_REMOVE, flags, (short) 0, fd, 0, udata, 0, 0, data);
    }

    /**
     * Returns a new {@code OP_CONNECT} {@link IOUringIoOps}.
     *
     * @param fd                                    the filedescriptor
     * @param flags                                 the flags.
     * @param acceptedAddressMemoryAddress          the memory address of the sockaddr_storage.
     * @parma acceptedAddressLengthMemoryAddress    the memory address of the length that will be updated once a new
     *                                              connection was accepted.
     * @param data                                  the data
     * @return                                      ops.
     */
    public static IOUringIoOps newAccept(int fd, int flags, int acceptFlags,  long acceptedAddressMemoryAddress,
                                         long acceptedAddressLengthMemoryAddress, short data) {
        return new IOUringIoOps(Native.IORING_OP_ACCEPT, flags, (short) 0, fd, acceptFlags,
                acceptedAddressMemoryAddress, 0, acceptedAddressLengthMemoryAddress, data);
    }

    /**
     * Returns a new {@code OP_WRITEV} {@link IOUringIoOps}.
     *
     * @param fd                                    the filedescriptor
     * @param flags                                 the flags.
     * @param writevFlags                           the writev flags.
     * @param memoryAddress                         the memory address of the io_vec array.
     * @param length                                the length of the io_vec array.
     * @param data                                  the data
     * @return                                      ops.
     */
    public static IOUringIoOps newWritev(int fd, int flags, int writevFlags, long memoryAddress,
                                         int length, short data) {
        return new IOUringIoOps(Native.IORING_OP_WRITEV, flags, (short) 0, fd,
                writevFlags, memoryAddress, length, 0, data);
    }

    /**
     * Returns a new {@code OP_WRITE} {@link IOUringIoOps}.
     *
     * @param fd                                    the filedescriptor
     * @param flags                                 the flags.
     * @param writeFlags                            the write flags.
     * @param memoryAddress                         the memory address of the buffer
     * @param length                                the length of the buffer.
     * @param data                                  the data
     * @return                                      ops.
     */
    public static IOUringIoOps newWrite(int fd, int flags, int writeFlags, long memoryAddress, int length, short data) {
        return new IOUringIoOps(Native.IORING_OP_WRITE, flags, (short) 0, fd,
                writeFlags, memoryAddress, length, 0, data);
    }

    /**
     * Returns a new {@code OP_RECV} {@link IOUringIoOps}.
     *
     * @param fd                                    the filedescriptor
     * @param flags                                 the flags.
     * @param recvFlags                             the recv flags.
     * @param memoryAddress                         the memory address of the buffer
     * @param length                                the length of the buffer.
     * @param data                                  the data
     * @return                                      ops.
     */
    public static IOUringIoOps newRecv(int fd, int flags, int recvFlags, long memoryAddress, int length, short data) {
        return new IOUringIoOps(Native.IORING_OP_RECV, flags, (short) 0, fd, recvFlags, memoryAddress, length, 0, data);
    }

    /**
     * Returns a new {@code OP_RECVMSG} {@link IOUringIoOps}.
     *
     * @param fd                                    the filedescriptor
     * @param flags                                 the flags.
     * @param msgFlags                              the recvmsg flags.
     * @param memoryAddress                         the memory address of the msghdr struct
     * @param data                                  the data
     * @return                                      ops.
     */
    public static IOUringIoOps newRecvmsg(int fd, int flags, int msgFlags, long memoryAddress, short data) {
        return new IOUringIoOps(Native.IORING_OP_RECVMSG, flags, (short) 0, fd, msgFlags, memoryAddress, 1, 0, data);
    }

    /**
     * Returns a new {@code OP_SEND} {@link IOUringIoOps}.
     *
     * @param fd                                    the filedescriptor
     * @param flags                                 the flags.
     * @param sendFlags                             the send flags.
     * @param memoryAddress                         the memory address of the buffer.
     * @param length                                the length of the buffer.
     * @param data                                  the data
     * @return                                      ops.
     */
    public static IOUringIoOps newSend(int fd, int flags, int sendFlags, long memoryAddress, int length, short data) {
        return new IOUringIoOps(Native.IORING_OP_SEND, flags, (short) 0, fd, sendFlags, memoryAddress, length, 0, data);
    }

    /**
     * Returns a new {@code OP_SEND} {@link IOUringIoOps}.
     *
     * @param fd                                    the filedescriptor
     * @param flags                                 the flags.
     * @param how                                   how the shutdown will be done.
     * @param data                                  the data
     * @return                                      ops.
     */
    public static IOUringIoOps newShutdown(int fd, int flags, int how, short data) {
        return new IOUringIoOps(Native.IORING_OP_SHUTDOWN, flags, (short) 0, fd, 0, 0, how, 0, data);
    }
}
