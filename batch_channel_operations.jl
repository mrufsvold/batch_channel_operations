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
    # shortcircuit for small vecs
    if length(vec) == 0
        return c
    elseif length(vec) == 1
        put!(c, @inbounds vec[begin])
        return c
    end
    Base.isbuffered(c) ? append_buffered(c, vec) : append_unbuffered(c, vec)
end

function Base.append!(c1::Channel, c2::Channel{T}) where {T}
    buff_len = max(c1.sz_max, c2.sz_max)
    if buff_len == 0
        return append_unbuffered(c1, c2)
    end

    buff = Vector{T}(undef, buff_len)
    while isopen(c2)
        take!(c2, buff_len, buff)
        append_buffered(c1, buff)
    end
    return c1
end

function append_unbuffered(c1::Channel, iter)
    for v in iter
        put!(c1, v)
    end
end

function append_buffered(c::Channel{T}, vec::AbstractArray) where {T}
    current_idx = firstindex(vec)
    final_idx = lastindex(vec)
    final_idx_plus_one = final_idx + 1

    elements_to_add = length(vec)
    # Increment channel n_avail eagerly (before push!) to count data in the
    # buffer as well as offers from tasks which are blocked in wait().
    Base._increment_n_avail(c, elements_to_add)
    while current_idx <= final_idx
        lock(c)
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
            # notify all, since some of the waiters may be on a "fetch" call.
            notify(c.cond_take, nothing, true, false)
            next_idx > final_idx && break
            current_idx = next_idx
        finally
            # Decrement the available items if this task had an exception before pushing the
            # item to the buffer (e.g., during `wait(c.cond_put)`):
            if elements_to_add > 0
                Base._increment_n_avail(c, -elements_to_add)
            end
            unlock(c)
        end
    end
    return c
end

function Base.take!(c::Channel{T}, n::Integer) where {T}
    return _take(c, n, Vector{T}(undef, n))
end

function Base.take!(c::Channel{T}, n::Integer, buffer::AbstractArray{T2}) where {T2,T<:T2}
    # buffer is user defined, so make sure it has the correct size
    if length(buffer) != n
        resize!(buffer, n)
    end
    return _take(c, n, buffer)
end

function _take(c::Channel{T}, n::Integer, buffer) where {T}
    buffered = Base.isbuffered(c)
    # short-circuit for small n
    if n == 0
        return buffer
    elseif n == 1
        @inbounds buffer[begin] =
            return buffer
    end

    buffered ? take_buffered(c, buffer, n) : take_unbuffered(c, buffer)

end

function take_buffered(c::Channel{T}, res::AbstractArray{T2}, n::Integer) where {T2,T<:T2}
    elements_taken = 0 # number of elements taken so far
    idx1 = firstindex(res)
    target_buffer_len = min(n, c.sz_max)
    lock(c)
    try
        while elements_taken < n && !(!isopen(c) && !isready(c))
            # wait until the channel has at least min_n elements or is full
            while length(c.data) < target_buffer_len && isopen(c)
                wait(c.cond_take)
            end
            # take as many elements as possible from the buffer
            n_to_take = min(n - elements_taken, length(c.data))
            idx_start = idx1 + elements_taken
            idx_end = idx_start + n_to_take - 1
            for (res_i, data_i) in Iterators.zip(idx_start:idx_end, eachindex(c.data))
                @inbounds res[res_i] = c.data[data_i]
            end
            deleteat!(c.data, 1:n_to_take)
            elements_taken += n_to_take
            Base._increment_n_avail(c, -n_to_take)
            foreach(_ -> notify(c.cond_put, nothing, true, false), 1:n_to_take)
        end
    finally
        unlock(c)
    end

    # if we broke early, we need to remove the extra slots from the result
    if elements_taken < n
        deleteat!(res, elements_taken+1:n)
    end
    return res
end

function take_unbuffered(c::Channel{T}, res::AbstractArray{T2}) where {T2,T<:T2}
    i = firstindex(res)
    lock(c)
    try
        for i in eachindex(res)
            @inbounds res[i] = try
                Base.take_unbuffered(c)
            catch e
                if isa(e, InvalidStateException) && e.state === :closed
                    deleteat!(res, i:lastindex(res))
                    break
                else
                    rethrow()
                end
            end
        end
    finally
        unlock(c)
    end
    res
end
