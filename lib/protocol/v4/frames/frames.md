The CQL binary protocol is a frame based protocol. Frames are defined as:

    0         8        16        24        32         40
    +---------+---------+---------+---------+---------+
    | version |  flags  |      stream       | opcode  |
    +---------+---------+---------+---------+---------+
    |                length                 |
    +---------+---------+---------+---------+
    |                                       |
    .            ...  body ...              .
    .                                       .
    .                                       .
    +----------------------------------------

The protocol is big-endian (network byte order).

Each frame contains a fixed size header (9 bytes) followed by a variable size
body. The header is described in Section 2. The content of the body depends
on the header opcode value (the body can in particular be empty for some
opcode values). The list of allowed opcodes is defined in Section 2.4 and the
details of each corresponding message are described Section 4.

The protocol distinguishes two types of frames: requests and responses. Requests
are those frames sent by the client to the server. Responses are those frames sent
by the server to the client. Note, however, that the protocol supports server pushes
(events) so a response does not necessarily come right after a client request.

Note to client implementors: client libraries should always assume that the
body of a given frame may contain more data than what is described in this
document. It will however always be safe to ignore the remainder of the frame
body in such cases. The reason is that this may enable extending the protocol
with optional features without needing to change the protocol version.
