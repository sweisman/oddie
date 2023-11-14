#ifndef MD5_H
#define MD5_H

// xxx add ifdef for 64 bits
typedef unsigned long uint32;

typedef struct
{
    uint32 buf[4];
    uint32 bits[2];
    unsigned char in[64];
} MD5Context;

extern void MD5Init();
extern void MD5Update();
extern void MD5Final();
extern void MD5Transform();

#endif
