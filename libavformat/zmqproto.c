/*
 * ZMQ Protocol Handler
 * Copyright (c) 2018 Critial Mention
 *
 * This file is part of FFmpeg.
 *
 * FFmpeg is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * FFmpeg is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with FFmpeg; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 */

/**
 * @file
 * ZMQP Protocol Handler
 */

#include "avformat.h"
#include "libavutil/avstring.h"
#include "url.h"
#include <zmq.h>

typedef struct ZMQContext {
    const AVClass *class;
    void *zmq;
    void *socket;
} ZMQContext;

static void zmqp_close(URLContext *ctx)
{
    ZMQContext *zmq = ctx->priv_data;
    if (zmq->socket && (ctx->flags & AVIO_FLAG_WRITE)) {
        zmq_send(zmq->socket, "", 0, 0);
    }
    zmq_close(zmq->socket);
    zmq_ctx_destroy(zmq->zmq);
}

static int zmqp_open(URLContext *ctx, const char *uri, int flags)
{
    ZMQContext *zmq = ctx->priv_data;
    av_strstart(uri, "zmq:", &uri);
    zmq->zmq = zmq_ctx_new();
    if (!zmq->zmq) {
        av_log(ctx, AV_LOG_ERROR,
                "Could not create ZMQ context: %s\n", zmq_strerror(errno));
        return AVERROR_EXTERNAL;
    }

    zmq->socket = zmq_socket(zmq->zmq, (flags & AVIO_FLAG_READ)?ZMQ_SUB:ZMQ_PUB);
    if (!zmq->socket) {
        av_log(ctx, AV_LOG_ERROR,
                "Could not create ZMQ socket: %s\n", zmq_strerror(errno));
        return AVERROR_EXTERNAL;
    }
    if (flags & AVIO_FLAG_READ) {
        if (zmq_connect(zmq->socket, uri) == -1) {
            av_log(ctx, AV_LOG_ERROR,
                    "Could not connect to ZMQ address '%s': %s\n",
                    uri, zmq_strerror(errno));
            return AVERROR_EXTERNAL;
        } else {
            av_log(ctx, AV_LOG_INFO, "Connected to read from %s\n", uri);
        }
        if (zmq_setsockopt(zmq->socket, ZMQ_SUBSCRIBE, "", 0) == -1) {
            av_log(ctx, AV_LOG_ERROR,
                    "Could not set ZMQ subscribe option: %s\n",
                    zmq_strerror(errno));
            return AVERROR_EXTERNAL;
        }
    } else {
        if (zmq_bind(zmq->socket, uri) == -1) {
            av_log(ctx, AV_LOG_ERROR,
                    "Could not bind ZMQ socket to address '%s': %s\n",
                    uri, zmq_strerror(errno));
            return AVERROR_EXTERNAL;
        } else {
            av_log(ctx, AV_LOG_INFO, "Connected to write to %s\n", uri);
        }
    }
    if (ctx->rw_timeout > 0) {
        av_log(ctx, AV_LOG_DEBUG,
                "Setting read timeout to %ld ms\n",
                (long)ctx->rw_timeout);
        if (zmq_setsockopt(zmq->socket,
                (flags & AVIO_FLAG_READ)?ZMQ_RCVTIMEO:ZMQ_SNDTIMEO,
                &ctx->rw_timeout, sizeof(int)) == -1) {
            av_log(ctx, AV_LOG_ERROR,
                    "Could not set ZMQ timeout option: %s\n",
                    zmq_strerror(errno));
        }
    }
    return 0;
}

static int zmqp_read(URLContext *ctx, uint8_t *buf, int size)
{
    ZMQContext *zmq = ctx->priv_data;
    int recv_size = 0;
    recv_size = zmq_recv(zmq->socket, buf, size, 0);
    if (recv_size == -1 && errno != EAGAIN) {
        av_log(ctx, AV_LOG_ERROR,
                "Could not receive message: %s\n", zmq_strerror(errno));
        return AVERROR_EXTERNAL;
    }
    if (recv_size == 0) {
        av_log(ctx, AV_LOG_ERROR, "EOF Signal of 0 byte message\n");
        zmqp_close(ctx);
        return AVERROR(EOF);
    }
    av_log(ctx, AV_LOG_DEBUG,
            "Got request for %d bytes, got %d bytes\n", size, recv_size);
    return recv_size;
}

static int zmqp_write(URLContext *ctx, const uint8_t *buf, int size)
{
    ZMQContext *zmq = ctx->priv_data;
    av_log(ctx, AV_LOG_DEBUG,
            "Got %d bytes. Writing to ZMQ\n", size);
    if (zmq_send(zmq->socket, buf, size, 0) == -1) {
        av_log(ctx, AV_LOG_ERROR,
                "Could not send data to ZMQ: %s\n",
                zmq_strerror(errno));
    }
    return size;
}

#if CONFIG_ZMQ_PROTOCOL
const URLProtocol ff_zmq_protocol = {
    .name              = "zmq",
    .url_open          = zmqp_open,
    .url_read          = zmqp_read,
    .url_write         = zmqp_write,
    .url_close         = zmqp_close,
    .flags             = URL_PROTOCOL_FLAG_NETWORK,
    .priv_data_size    = sizeof(ZMQContext),
    .default_whitelist = "crypto,file,http,https,httpproxy,rtmp,tcp,tls"
};
#endif
