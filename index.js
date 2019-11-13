var raf = require('random-access-file')
var secretStorage = require('@dwebjs/secret-storage')
var multi = require('multi-random-access')
var messages = require('append-tree/messages')
var stat = require('ddrive/lib/messages').Stat
var path = require('path')

module.exports = function(dir, opts) {
    if (!opts) opts = {}
    var prefix = opts.prefix || '.dweb/'
    return {
        metadata: function(name, metaOpts) {
            if (typeof dir === 'function') return dir(prefix + 'metadata.' + name)
            if (name === 'secret_key') return secretStorage(opts.secretDir)(path.join(dir, prefix + 'metadata.ogd'), { key: metaOpts.key, revelationKey: metaOpts.revelationKey })
            return raf(path.join(dir, prefix + 'metadata.' + name))
        },
        content: function(name, contentOpts, vault) {
            if (!vault) vault = contentOpts
            if (name === 'data') return createStorage(vault, dir)
            if (typeof dir === 'function') return dir(prefix + 'content.' + name)
            return raf(path.join(dir, prefix + 'content.' + name))
        }
    }
}

function createStorage(vault, dir) {
    if (!vault.latest) throw new Error('Currently only "latest" mode is supported.')

    var latest = vault.latest
    var head = null
    var storage = multi({ limit: 128 }, locate)

    // TODO: this should be split into two events, 'appending' and 'append'
    vault.on('appending', onappending)
    vault.on('append', onappend)

    return storage

    function onappend(name, opts) {
        if (head) head.end = vault.content.byteLength
    }

    function onappending(name, opts) {
        if (head) head.end = vault.content.byteLength

        var v = latest ? '' : '.' + vault.metadata.length

        head = {
            start: vault.content.byteLength,
            end: Infinity,
            storage: file(name + v)
        }

        storage.add(head)
    }

    function locate(offset, cb) {
        vault.ready(function(err) {
            if (err) return cb(err)

            find(vault.metadata, offset, function(err, node, st, index) {
                if (err) return cb(err)
                if (!node) return cb(new Error('Could not locate data'))

                var v = latest ? '' : '.' + index

                cb(null, {
                    start: st.byteOffset,
                    end: st.byteOffset + st.size,
                    storage: file(node.name + v)
                })
            })
        })
    }

    function file(name) {
        if (typeof dir === 'function') return dir(name)
        return raf(name, { directory: dir, rmdir: true })
    }
}

function get(metadata, btm, seq, cb) {
    if (seq < btm) return cb(null, -1, null)

    // TODO: this can be done a lot faster using the ddatabase internal iterators, expose!
    var i = seq
    while (!metadata.has(i) && i > btm) i--
        if (!metadata.has(i)) return cb(null, -1, null)

    metadata.get(i, { valueEncoding: messages.Node }, function(err, node) {
        if (err) return cb(err)

        var st = node.value && stat.decode(node.value)

        if (!node.value || (!st.offset && !st.blocks) || (!st.byteOffset && !st.blocks)) {
            return get(metadata, btm, i - 1, cb) // TODO: check the index instead for fast lookup
        }

        cb(null, i, node, st)
    })
}

function find(metadata, bytes, cb) {
    var top = metadata.length - 1
    var btm = 1
    var mid = Math.floor((top + btm) / 2)

    get(metadata, btm, mid, function loop(err, actual, node, st) {
        if (err) return cb(err)

        var oldMid = mid

        if (!node) {
            btm = mid
            mid = Math.floor((top + btm) / 2)
        } else {
            var start = st.byteOffset
            var end = st.byteOffset + st.size

            if (start <= bytes && bytes < end) return cb(null, node, st, actual)
            if (top <= btm) return cb(null, null, null, -1)

            if (bytes < start) {
                top = mid
                mid = Math.floor((top + btm) / 2)
            } else {
                btm = mid
                mid = Math.floor((top + btm) / 2)
            }
        }

        if (mid === oldMid) {
            if (btm < top) mid++
                else return cb(null, null, null, -1)
        }

        get(metadata, btm, mid, loop)
    })
}