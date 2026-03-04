#include "MysqlBufferProvider.h"

namespace galay::mysql
{

MysqlRingBufferProvider::MysqlRingBufferProvider(size_t capacity)
    : m_buffer(capacity)
{
}

size_t MysqlRingBufferProvider::getWriteIovecs(struct iovec* out, size_t max_iovecs)
{
    return m_buffer.getWriteIovecs(out, max_iovecs);
}

size_t MysqlRingBufferProvider::getReadIovecs(struct iovec* out, size_t max_iovecs) const
{
    return m_buffer.getReadIovecs(out, max_iovecs);
}

void MysqlRingBufferProvider::produce(size_t len)
{
    m_buffer.produce(len);
}

void MysqlRingBufferProvider::consume(size_t len)
{
    m_buffer.consume(len);
}

void MysqlRingBufferProvider::clear()
{
    m_buffer.clear();
}

MysqlBufferHandle::MysqlBufferHandle(size_t capacity,
                                     std::shared_ptr<MysqlBufferProvider> provider)
{
    if (provider) {
        m_provider = std::move(provider);
    } else {
        m_provider = std::make_shared<MysqlRingBufferProvider>(capacity);
    }
}

} // namespace galay::mysql
