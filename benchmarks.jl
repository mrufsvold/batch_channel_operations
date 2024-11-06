using Chairmarks: @b
using Test
includet("batch_channel_operations.jl")
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


@testset "append!(ch, itr)" begin
    # buffered channel
    c = Channel(3)
    @test append!(c, 1:3) === c
    @test Base.n_avail(c) == 3
    close(c)
    @test collect(c) == [1, 2, 3]
    @test Base.n_avail(c) == 0
    @test_throws InvalidStateException append!(c, 1:3)

    c = Channel(3) do c
        append!(c, 1:5)
    end
    @test collect(c) == [1, 2, 3, 4, 5]
    
    # unbuffered channel
    c = Channel()
    @async begin
        append!(c, 1:3)
        close(c)
    end
    wait(c)
    @test Base.n_avail(c) == 1
    @test collect(c) == [1, 2, 3]
    @test Base.n_avail(c) == 0
    @test_throws InvalidStateException append!(c, 1:3)

    # appending channels
end
c1 = Channel(3) do c
    append!(c, 1:5)
end
c2 = Channel(3)

t = @async collect(c2)

# this is still blocking 
@run append!(c2, c1)
close(c2)
fetch(t)

do c
    append!(c, c1)
end
# @test collect(c2) == [1, 2, 3, 4, 5]

# @testset "take!(ch, n)" begin
    c = Channel(3)
    append!(c, 1:3)
    @test take!(c, 3) == [1, 2, 3]
    @test Base.n_avail(c) == 0

    @async begin
        append!(c, 1:3)
        close(c)
    end
    @test take!(c, 5) == [1, 2, 3]
    
end
