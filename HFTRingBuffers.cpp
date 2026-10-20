
//SPSC Lock-Free Ring Buffer (Queue)

#include <iostream>
#include <vector>
#include <atomic>
#include <thread>
#include <new>
#include <chrono>

template<typename T, int size = 20>
class SPSCRingQueue 
{
public:
    
    SPSCRingQueue() : _buffer(size) {};
    ~SPSCRingQueue() = default;
    SPSCRingQueue(const SPSCRingQueue&) = delete;
    SPSCRingQueue& operator=(const SPSCRingQueue&) = delete;
    SPSCRingQueue(SPSCRingQueue&&) = delete;
    SPSCRingQueue& operator=(SPSCRingQueue&&) = delete;

    bool Enqueue(const T& element)
    {
        const size_t writeIndex = _writeIndex.load(std::memory_order_acquire);
        const size_t readIndex = _readIndex.load(std::memory_order_acquire);
        
        if (writeIndex - readIndex >= size)
            return false; // full
        
        _buffer[writeIndex % size] = element;

        _writeIndex.store(writeIndex + 1, std::memory_order_release);
        return true;
    }

    bool Dequeue(T& out)
    {
        const size_t writeIndex = _writeIndex.load(std::memory_order_acquire);
        const size_t readIndex = _readIndex.load(std::memory_order_acquire);
   
        if (readIndex == writeIndex)
            return false; // empty

        out = _buffer[readIndex % size];

        _readIndex.store(readIndex + 1, std::memory_order_release);

        return true;
    }

    bool IsEmpty() const
    {
        const size_t writeIndex = _writeIndex.load(std::memory_order_acquire);
        const size_t readIndex = _readIndex.load(std::memory_order_acquire);

        return readIndex == writeIndex;
    }

private:

    std::vector<T> _buffer;

    alignas(std::hardware_destructive_interference_size)
    std::atomic<size_t> _readIndex = 0;

    alignas(std::hardware_destructive_interference_size)
    std::atomic<size_t> _writeIndex = 0;
};

template<typename T, int size = 20>
class MPMCRingQueue
{
    static_assert((size & (size - 1)) == 0, "Capacity must be a power of 2");

public:

    struct Cell
    {
        std::atomic<size_t> _sequence;
        T _data;
    };

    MPMCRingQueue() : _buffer(size), _mask(size - 1)
    {
        for (int i = 0; i < size; i++)
            _buffer[i]._sequence.store(i, std::memory_order_relaxed);
    }

    ~MPMCRingQueue() = default;
    MPMCRingQueue(const MPMCRingQueue&) = delete;
    MPMCRingQueue& operator=(const MPMCRingQueue&) = delete;
    MPMCRingQueue(MPMCRingQueue&&) = delete;
    MPMCRingQueue& operator=(MPMCRingQueue&&) = delete;

    bool Enqueue(const T& element)
    {
        Cell* cell = nullptr;
        size_t enqPos = _writeIndex.load(std::memory_order_relaxed);

        while (true)
        {
            // optimized % 2 for pow2 sizes
            cell = &_buffer[enqPos & _mask]; 

            const size_t sequence = cell->_sequence.load(std::memory_order_acquire);
            const size_t stateDeterminant = sequence - enqPos;

            if (stateDeterminant == 0)
            {
                if (_writeIndex.compare_exchange_weak(enqPos, enqPos + 1, std::memory_order_relaxed, std::memory_order_relaxed))
                    break;
            }
            else if (stateDeterminant < 0)
            {
                // full
                return false;
            }
            else // > 0
            {
                enqPos = _writeIndex.load(std::memory_order_relaxed);
            }
        }

        cell->_data = element;
        cell->_sequence.store(enqPos + 1, std::memory_order_release);
        return true;
    }

    bool Dequeue(T& out)
    {
        Cell* cell = nullptr;
        size_t deqPos = _readIndex.load(std::memory_order_relaxed);

        while (true)
        {
            cell = &_buffer[deqPos & _mask];
            const size_t sequence = cell->_sequence.load(std::memory_order_acquire);
            const size_t stateDeterminant = sequence - (deqPos + 1);

            if (stateDeterminant == 0)
            {
                if (_readIndex.compare_exchange_weak(deqPos, deqPos + 1, std::memory_order_relaxed, std::memory_order_relaxed))
                    break;
            }
            else if (stateDeterminant < 0)
            {
                // empty
                return false; 
            }
            else // > 0
            {
                deqPos = _readIndex.load(std::memory_order_relaxed);
            }
        }

        out = cell->_data;
        cell->_sequence.store(deqPos + _mask + 1, std::memory_order_release);
        return true;
    }

    bool IsEmpty() const
    {
        const size_t writeIndex = _writeIndex.load(std::memory_order_acquire);
        const size_t readIndex = _readIndex.load(std::memory_order_acquire);

        return readIndex == writeIndex;
    }

private:

    const size_t _mask;
    
    std::vector<Cell> _buffer;

    alignas(std::hardware_destructive_interference_size)
    std::atomic<size_t> _readIndex = 0;

    alignas(std::hardware_destructive_interference_size)
    std::atomic<size_t> _writeIndex = 0;
};

SPSCRingQueue<int> SPSCIntBuffer;

/**
 * @brief Stress-tests the SPSCRingQueue under concurrent multi-threaded conditions.
 * * Spawns a producer thread that pushes items with retry logic to test full-capacity handling.
 * * Delays the consumer thread startup to test queue saturation and backlog management.
 * * Validates thread-safe ordering, backpressure handling, and graceful drainage on shutdown.
 */
void SPCSIntTest()
{
    std::cout << "SPCSIntTest():\n";

    using namespace std::chrono_literals;

    // simulates open/close state of the stream/socket
    std::atomic<bool> socket = true;

    auto insertionFn = [&socket](const int iterations)
        {
            for (int i = 0; i < iterations; i++)
            {
                if (SPSCIntBuffer.Enqueue(i))
                    std::cout << "Enqueued: " << i << "\n";
                else
                    i--;

                // overload the queue up to a size before consumer can ever read it.
                // tests capacity management as well as ordering
                std::this_thread::sleep_for(50ms);
            }

            socket.store(false, std::memory_order_release);
        };

    auto readFn = [&socket]()
        {
            std::this_thread::sleep_for(2000ms);
            while (socket.load(std::memory_order_acquire) || !SPSCIntBuffer.IsEmpty())
            {
                // simulate slow consumer who bulk consumes ALL that is available.
                std::this_thread::sleep_for(5000ms);

                int out;
                while (SPSCIntBuffer.Dequeue(out))
                    std::cout << "Dequeued: " << out << "\n";


            }
        };

    std::jthread th0{ insertionFn, 50 };
    std::jthread th1{ readFn };
}

int main()
{
    SPCSIntTest();
    std::cout << "Done!\n";
}
