"""
    append!(c::Channel, vec)

Append all items in `vec` to the channel `c`. If the channel is buffered, this operation requires
fewer `lock` operations than individual `put!`s. Blocks if the channel is full.

For unbuffered channels, blocks until a [`take!`](@ref) is performed by a different
task.

!!! compat "Julia 1.1"
    `v` now gets converted to the channel's type with [`convert`](@ref) as `put!` is called.
"""
function Base.append!(c::Channel{T}, vec::AbstractArray) where {T}
    current_idx = firstindex(vec)
    final_idx = lastindex(vec)
    final_idx_plus_one = final_idx + 1

    elements_to_add = length(vec)
    # Increment channel n_avail eagerly (before push!) to count data in the
    # buffer as well as offers from tasks which are blocked in wait().
    Base._increment_n_avail(c, elements_to_add)
    while current_idx <= final_idx
        lock(c)
        did_buffer = false
        try
            while length(c.data) == c.sz_max
                Base.check_channel_state(c)
                wait(c.cond_put)
            end
            # Grab a chunk of items that will fit in the channel's buffer
            available_space = c.sz_max - length(c.data)
            next_idx = min(final_idx_plus_one, current_idx + available_space)
            chunk = Iterators.map(x -> convert(T, x), view(vec, current_idx:(next_idx-1)))

            Base.check_channel_state(c)
            append!(c.data, chunk)
            # We successfully added chunk, so decrement our elements to add in case of
            # errors
            elements_to_add -= next_idx - current_idx
            did_buffer = true
            notify(c.cond_take, nothing, true, false)
            # notify all, since some of the waiters may be on a "fetch" call.
            next_idx > final_idx && break
            current_idx = next_idx
        finally
            # Decrement the available items if this task had an exception before pushing the
            # item to the buffer (e.g., during `wait(c.cond_put)`):
            did_buffer || Base._increment_n_avail(c, -elements_to_add)
            unlock(c)
        end
    end
    return c
end


function take_batch!(c::Channel{T}, n) where {T}
    if c.sz_max < n
        throw(ArgumentError("Batch size, $n, is too large for a channel with buffer length $(c.sz_max)"))
    end
    lock(c)
    try
        while isempty(c.data)
            Base.check_channel_state(c)
            wait(c.cond_take)
        end

        take_n = min(n, length(c.data))
        ret = Vector{T}(undef, take_n)
        @inbounds for i in eachindex(ret)
            ret[i] = c.data[i]
        end
        foreach(_ -> popfirst!(c.data), 1:take_n)
        Base._increment_n_avail(c, -take_n)
        notify(c.cond_put, nothing, false, false) # notify only one, since only one slot has become available for a put!.
        return ret
    finally
        unlock(c)
    end
end

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

function bench_batch(item_n, buffer_len)
    items = collect(1:item_n)
    ch = Channel{Int}(buffer_len)
    task_n = Threads.nthreads()
    res = Vector{Int}(undef, item_n * task_n)

    ch = Channel{Int}(buffer_len)
    for _ in 1:task_n
        Threads.@spawn begin
            i = 1
            while i <= item_n
                append!(ch, @view items[i:min(i + buffer_len - 1, item_n)])
                i += buffer_len
            end
        end
    end

    @sync for i in Base.OneTo(task_n)
        Threads.@spawn let offset = (i - 1) * item_n
            batch = take_batch!(ch, buffer_len)
            batch_len = length(batch)
            batch_i = 1
            for j in Base.OneTo(item_n)
                if batch_i > batch_len
                    batch = take_batch!(ch, buffer_len)
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
@b bench_basic(10000, 10)
@b bench_batch(10000, 10)

@b bench_basic(10000, 100)
@b bench_batch(10000, 100)

@b bench_basic(10000, 1000)
@b bench_batch(10000, 1000)

@profview_allocs bench_batch(100000, 1000)
