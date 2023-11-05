# File downlink protocol

This project aims to be a transmission protocol for file downlink over an unreliable asymmetrical communication channel with very sparse acknowledgements. I made a custom protocol because all the others I could find relied on more frequent acknowledgements, and the majority of the protocol was atomic file handling code anyway rather than pure computational logic.

## Project structure

The workspace has the following projects:

### common

This is any common code that's used by both the sender and receiver. For example, it contains the structs for the data chunks and the control packets, file header, etc.

### sender

This is the sender application. It organizes any provided files into chunks and provides an api to get the next stream of chunks to send, as well as receiving control events. The sender is multithreaded and designed to be run as an asynchronous background service to the overall application.

### receiver

This is the receiver application. It has functions to process received chunks and control events, and to write the received file to disk. The receiver is a simple single threaded structure, designed to be disposable, with no internal state, other than a flushable queue of control events.

## Protocol

The protocol consists of two parts, the application layer and the transport layer. The application layer is the part that handles the file chunking and reassembly, and the transport layer is the part that handles the transmission of the chunks and control packets.

### Application layer

The 2 main components of the application layer are "chunks", which get sent to earth, and "control events", which may periodically be to the satellite from earth to provide status updates (e.g. chunk receival status).

Each file gets divided into the following parts:
- A generated UUID, which is used as the primary key for the file
- A file header (which itself is a chunk), which contains the file name, file size, the number of chunks in the file, and some other metadata
- A series of chunks, each containing file data

Each chunk contains the following:
- The file UUID
- The chunk index (or a special index if it's the header chunk)
- The chunk data

For any fully received chunk, control events are generated containing the file UUID and the chunk index, to let the satellite know that it was properly received.

A file is fully received when the header of the file has been received, and all the data chunks (based on the header's chunk count) have also been received.

### Transport layer

TBD
