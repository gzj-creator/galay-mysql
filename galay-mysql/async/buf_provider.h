/**
 * @file buf_provider.h
 * @brief MySQL缓冲区提供者接口与环形缓冲区实现
 * @author galay-mysql
 * @version 1.0.0
 *
 * @details 定义了MySQL协议读写所需的缓冲区抽象接口(MysqlBufferProvider)，
 *          基于环形缓冲区的具体实现(MysqlRingBufferProvider)，
 *          以及用于持有和访问缓冲区的句柄类(MysqlBufferHandle)。
 */

#ifndef GALAY_MYSQL_BUFFER_PROVIDER_H
#define GALAY_MYSQL_BUFFER_PROVIDER_H

#include <galay-kernel/common/buffer.h>

#include <cstddef>
#include <memory>
#include <sys/uio.h>

namespace galay::mysql
{

/**
 * @brief MySQL缓冲区提供者抽象接口
 * @details 定义了MySQL协议层与底层缓冲区之间的交互接口，
 *          支持通过iovec进行分散/聚集IO操作。
 */
class MysqlBufferProvider
{
public:
    virtual ~MysqlBufferProvider() = default;

    /**
     * @brief 获取可写区域的iovec数组
     * @param[out] out 输出的iovec数组
     * @param max_iovecs 最大iovec数量，默认为2
     * @return 实际填充的iovec数量
     */
    virtual size_t getWriteIovecs(struct iovec* out, size_t max_iovecs = 2) = 0;

    /**
     * @brief 获取可读区域的iovec数组
     * @param[out] out 输出的iovec数组
     * @param max_iovecs 最大iovec数量，默认为2
     * @return 实际填充的iovec数量
     */
    virtual size_t getReadIovecs(struct iovec* out, size_t max_iovecs = 2) const = 0;

    /**
     * @brief 标记已写入的数据长度
     * @param len 写入的字节数
     */
    virtual void produce(size_t len) = 0;

    /**
     * @brief 标记已读取的数据长度
     * @param len 读取的字节数
     */
    virtual void consume(size_t len) = 0;

    /**
     * @brief 清空缓冲区
     */
    virtual void clear() = 0;
};

/**
 * @brief 基于环形缓冲区的MySQL缓冲区提供者
 * @details 使用galay-kernel的RingBuffer实现，适用于MySQL协议的读写缓冲。
 */
class MysqlRingBufferProvider final : public MysqlBufferProvider
{
public:
    /**
     * @brief 构造指定容量的环形缓冲区
     * @param capacity 缓冲区容量（字节）
     */
    explicit MysqlRingBufferProvider(size_t capacity);

    size_t getWriteIovecs(struct iovec* out, size_t max_iovecs = 2) override;
    size_t getReadIovecs(struct iovec* out, size_t max_iovecs = 2) const override;
    void produce(size_t len) override;
    void consume(size_t len) override;
    void clear() override;

private:
    galay::kernel::RingBuffer m_buffer; ///< 底层环形缓冲区
};

/**
 * @brief MySQL缓冲区句柄
 * @details 持有MysqlBufferProvider的shared_ptr，提供对缓冲区的便捷访问。
 *          支持拷贝和移动语义，允许多个句柄共享同一底层缓冲区。
 */
class MysqlBufferHandle
{
public:
    /**
     * @brief 构造缓冲区句柄
     * @param capacity 缓冲区容量（字节）
     * @param provider 自定义缓冲区提供者，为nullptr时使用默认的环形缓冲区
     */
    explicit MysqlBufferHandle(size_t capacity = galay::kernel::RingBuffer::kDefaultCapacity,
                               std::shared_ptr<MysqlBufferProvider> provider = nullptr);

    MysqlBufferHandle(const MysqlBufferHandle&) = default;
    MysqlBufferHandle& operator=(const MysqlBufferHandle&) = default;
    MysqlBufferHandle(MysqlBufferHandle&&) noexcept = default;
    MysqlBufferHandle& operator=(MysqlBufferHandle&&) noexcept = default;
    ~MysqlBufferHandle() = default;

    /**
     * @brief 获取可写区域的iovec数组
     * @param[out] out 输出的iovec数组
     * @param max_iovecs 最大iovec数量
     * @return 实际填充的iovec数量
     */
    size_t getWriteIovecs(struct iovec* out, size_t max_iovecs = 2)
    {
        return m_provider->getWriteIovecs(out, max_iovecs);
    }

    /**
     * @brief 获取可读区域的iovec数组
     * @param[out] out 输出的iovec数组
     * @param max_iovecs 最大iovec数量
     * @return 实际填充的iovec数量
     */
    size_t getReadIovecs(struct iovec* out, size_t max_iovecs = 2) const
    {
        return m_provider->getReadIovecs(out, max_iovecs);
    }

    void produce(size_t len) { m_provider->produce(len); } ///< 标记已写入len字节
    void consume(size_t len) { m_provider->consume(len); } ///< 标记已读取len字节
    void clear() { m_provider->clear(); }                   ///< 清空缓冲区

    MysqlBufferProvider& provider() { return *m_provider; }             ///< 获取缓冲区提供者引用
    const MysqlBufferProvider& provider() const { return *m_provider; } ///< 获取缓冲区提供者常量引用
    std::shared_ptr<MysqlBufferProvider> shared() const { return m_provider; } ///< 获取底层shared_ptr

private:
    std::shared_ptr<MysqlBufferProvider> m_provider; ///< 底层缓冲区提供者
};

} // namespace galay::mysql

#endif // GALAY_MYSQL_BUFFER_PROVIDER_H
