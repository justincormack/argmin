## Streaming

Right now we are always reading objects into memory before PUT, and in similar 
operations like copy. For this reason we have hardcoded a maximum object size 
of 256MB. We need to remove this limit, as we need to match the AWS limit of 
5GB for a single object or chunk for multipart objects. This is much too big to 
keep in memory. So we need to support streaming objects. This will be quite a 
large change so we need to architect, plan and implement it carefully.

We have the very important design constraint that PGs are managing concurrency 
correctness, as each one serialises operations. This must remain. We support 
high performance by having a large number of PGs which spreads the work. 
However, we were not really thinking about streaming with the design. If we 
were to directly stream operations from a client through to the PG and write to 
disk directly from the stream, we could have a lot of very slow clients tie up 
the PGs completely, eg slow IoT clients writing large objects very slowly. Even 
with say 100 PGs per disk this could be an issue. Increasing the number of PGs 
does not help, as if your request is placed on the same PG it could be stuck 
for a long time behind a slow device writing a big file.

So I think we need to make some changes to address this issue. My initial 
thought, although open to discussing other options, is that we buffer requests 
in smallish buffers in the front end (not sure about size, probably at least 
64k?) and then pass these chunks to the PGs when they are complete (reach size 
or finish). So the PGs get a stream of these chunks. But they need to be able 
to then interleave writing these chunks between multiple different PUT 
requests, but without breaking any concurrency constraints. So instead of 
purely sequential, the PG has to be able to handle multiple (for some fixed 
number) non interfering transactions at the same time, eg PUTs for different 
keys.

Although I talked about this in terms of PUT, we would need a similar scheme 
for GET to deal with slow readers, and all the other operations. Some other 
thoughts, we recently implemented aws chunked streaming, this has a minimum 
chunk size of 8k although recommends at least 64k, which perhaps gives some 
idea of size of streaming chunks, although of course we have to support non 
chunked uploads too.

This is a complex change so we need a detailed plan, and we need to be able to 
validate correctness as it touches key concurrency correctness guarantees that 
we require to be correct.

