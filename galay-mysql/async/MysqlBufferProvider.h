#ifndef GALAY_MYSQL_BUFFER_PROVIDER_H
#define GALAY_MYSQL_BUFFER_PROVIDER_H

#include <galay-kernel/common/Buffer.h>

#include <cstddef>
#include <memory>
#include <sys/uio.h>

namespace galay::mysql
{

class MysqlBufferProvider
{
public:
    virtual ~MysqlBufferProvider() = default;

    virtual size_t getWriteIovecs(struct iovec* out, size_t max_iovecs = 2) = 0;
    virtual size_t getReadIovecs(struct iovec* out, size_t max_iovecs = 2) const = 0;
    virtual void produce(size_t len) = 0;
    virtual void consume(size_t len) = 0;
    virtual void clear() = 0;
};

class MysqlRingBufferProvider final : public MysqlBufferProvider
{
public:
    explicit MysqlRingBufferProvider(size_t capacity);

    size_t getWriteIovecs(struct iovec* out, size_t max_iovecs = 2) override;
    size_t getReadIovecs(struct iovec* out, size_t max_iovecs = 2) const override;
    void produce(size_t len) override;
    void consume(size_t len) override;
    void clear() override;

private:
    galay::kernel::RingBuffer m_buffer;
};

class MysqlBufferHandle
{
public:
    explicit MysqlBufferHandle(size_t capacity = galay::kernel::RingBuffer::kDefaultCapacity,
                               std::shared_ptr<MysqlBufferProvider> provider = nullptr);

    MysqlBufferHandle(const MysqlBufferHandle&) = default;
    MysqlBufferHandle& operator=(const MysqlBufferHandle&) = default;
    MysqlBufferHandle(MysqlBufferHandle&&) noexcept = default;
    MysqlBufferHandle& operator=(MysqlBufferHandle&&) noexcept = default;
    ~MysqlBufferHandle() = default;

    size_t getWriteIovecs(struct iovec* out, size_t max_iovecs = 2)
    {
        return m_provider->getWriteIovecs(out, max_iovecs);
    }

    size_t getReadIovecs(struct iovec* out, size_t max_iovecs = 2) const
    {
        return m_provider->getReadIovecs(out, max_iovecs);
    }

    void produce(size_t len) { m_provider->produce(len); }
    void consume(size_t len) { m_provider->consume(len); }
    void clear() { m_provider->clear(); }

    MysqlBufferProvider& provider() { return *m_provider; }
    const MysqlBufferProvider& provider() const { return *m_provider; }
    std::shared_ptr<MysqlBufferProvider> shared() const { return m_provider; }

private:
    std::shared_ptr<MysqlBufferProvider> m_provider;
};

} // namespace galay::mysql

#endif // GALAY_MYSQL_BUFFER_PROVIDER_H
