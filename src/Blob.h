#ifndef Blob_h
#define Blob_h

#include <string.h>
#include <string>

#include "rct/Serializer.h"

class Blob
{
public:
    Blob() {}
    Blob(const char *data, size_t len) : mBuffer(data, len) {}

    const char *data() const
    {
        return mBuffer.data();
    }

    size_t size() const
    {
        return mBuffer.size();
    }

    char operator[](size_t n) const
    {
        return mBuffer[n];
    }

    char &operator[](size_t n)
    {
        return mBuffer[n];
    }

    void clear()
    {
        mBuffer.clear();
    }

    void resize(size_t len)
    {
        mBuffer.resize(len);
    }

    Blob &assign(const Blob &b)
    {
        mBuffer.assign(b.mBuffer);
        return *this;
    }

    Blob &assign(const char *data, size_t len)
    {
        mBuffer.assign(data, len);
        return *this;
    }

    Blob &append(const Blob &b)
    {
        mBuffer.append(b.mBuffer);
        return *this;
    }

    Blob &append(const char *data, size_t len)
    {
        mBuffer.append(data, len);
        return *this;
    }

    int compare(const Blob &b) const
    {
        return mBuffer.compare(b.mBuffer);
    }

    bool startsWith(const Blob &prefix) const
    {
        return size() >= prefix.size() &&
               !memcmp(data(), prefix.data(), prefix.size());
    }

private:
    std::string mBuffer;
};

class BlobBuffer : public Serializer::Buffer
{
public:
    BlobBuffer(Blob &out)
        : mBlob(&out)
    {}

    virtual bool write(const void *data, int len) override
    {
        mBlob->append(static_cast<const char*>(data), len);
        return true;
    }
    virtual int pos() const override
    {
        return mBlob->size();
    }
private:
    Blob *mBlob;
};

static inline Serializer getBlobSerializer(Blob &b)
{
    return Serializer(std::unique_ptr<BlobBuffer>(new BlobBuffer(b)));
}

static inline Deserializer getBlobDeserializer(const Blob &b)
{
    return Deserializer(b.data(), b.size());
}

template <> inline Serializer &operator<<(Serializer &s, const Blob &b)
{
    size_t size = b.size();
    s << size;
    if (size) {
        s.write(b.data(), size);
    }
    return s;
}

template <> inline Deserializer &operator>>(Deserializer &s, Blob &b)
{
    size_t size;
    s >> size;
    b.resize(size);
    if (size) {
        s.read(&b[0], size);
    }
    return s;
}

#endif
