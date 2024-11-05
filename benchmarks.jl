using Chairmarks: @b
using Test

function bench_basic(item_n, buffer_len)
    items = collect(1:item_n)
    ch = Channel{Int}(buffer_len)
    task_n = Threads.nthreads()
    res = Vector{Int}(undef, item_n * task_n)

    for _ in 1:task_n
        Threads.@spawn begin
            for j in items
                put!(ch, j)
            end
        end
    end

    @sync for i in Base.OneTo(task_n)
        Threads.@spawn let offset = (i - 1) * item_n
            for j in Base.OneTo(item_n)
                x = take!(ch)
                res[offset+j] = x
            end
        end
    end
    res
end

function bench_batch(item_n, buffer_len, batch_size=buffer_len > 0 ? buffer_len : 100)
    items = collect(1:item_n)
    ch = Channel{Int}(buffer_len)
    task_n = Threads.nthreads()
    res = Vector{Int}(undef, item_n * task_n)

    for _ in 1:task_n
        Threads.@spawn begin
            i = 1
            while i <= item_n
                chunk = @view items[i:min(i + batch_size - 1, item_n)]
                append!(ch, chunk)
                i += batch_size
            end
        end
    end

    @sync for i in Base.OneTo(task_n)
        Threads.@spawn let offset = (i - 1) * item_n
            buff = Vector{Int}(undef, batch_size)
            batch = take!(ch, batch_size, buff)
            batch_len = length(batch)
            batch_i = 1
            for j in Base.OneTo(item_n)
                if batch_i > batch_len
                    batch = take!(ch, batch_size, buff)
                    batch_i = 1
                    batch_len = length(batch)
                end
                x = batch[batch_i]
                res[offset+j] = x
                batch_i += 1
            end
        end

    end
    res
end

GC.gc()
# Check to make sure the implementation is correct
@test sort!(bench_basic(10000, 10)) == sort!(bench_batch(10000, 10))

@b bench_basic(10000, 0)
@b bench_batch(10000, 0)

@b bench_basic(10000, 10)
@b bench_batch(10000, 10)

@b bench_basic(10000, 100)
@b bench_batch(10000, 100)

@b bench_basic(10000, 1000)
@b bench_batch(10000, 1000)

ch = Channel{Int}(10)
ch2 = Channel{Int}(10) do c
    append!(c, 1:1000)
end

Threads.@spawn append!(ch, ch2)
take!(ch, 1000)
