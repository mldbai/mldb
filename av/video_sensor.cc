/*
 * Copyright (c) 2012 Stefano Sabatini
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

extern "C" {

#include <libavutil/imgutils.h>
#include <libavutil/samplefmt.h>
#include <libavutil/opt.h>
#include <libavcodec/avcodec.h>
#include <libavformat/avformat.h>
#include <libavdevice/avdevice.h>

}


static AVFormatContext *fmt_ctx = NULL;
static AVCodecContext *video_dec_ctx = NULL;
static AVStream *video_stream = NULL;
static const char *src_filename = NULL;
static const char *video_dst_filename = NULL;
static FILE *video_dst_file = NULL;

static uint8_t *video_dst_data[4] = {NULL};
static int video_dst_linesize[4];
static int video_dst_bufsize;

static int video_stream_idx = -1;
static AVFrame *frame = NULL;
static AVPacket pkt;
static int video_frame_count = 0;

/**
 * Copied from libav/cmdutils.c because unlike FFmpeg, Libav does not export
 * this function in the public API.
 */
const char *media_type_string(enum AVMediaType media_type)
{
    switch (media_type) {
    case AVMEDIA_TYPE_VIDEO:      return "video";
    case AVMEDIA_TYPE_AUDIO:      return "audio";
    case AVMEDIA_TYPE_DATA:       return "data";
    case AVMEDIA_TYPE_SUBTITLE:   return "subtitle";
    case AVMEDIA_TYPE_ATTACHMENT: return "attachment";
    default:                      return "unknown";
    }
}

static int decode_packet(int *got_frame, int cached)
{
    int ret = 0;
    int decoded = pkt.size;

    *got_frame = 0;

    if (pkt.stream_index == video_stream_idx) {
        // decode video frame
        ret = avcodec_decode_video2(video_dec_ctx, frame, got_frame, &pkt);
        if (ret < 0) {
            // FFmpeg users should use av_err2str
            char errbuf[128];
            av_strerror(ret, errbuf, sizeof(errbuf));
            fprintf(stderr, "Error decoding video frame (%s)\n", errbuf);
            return ret;
        }

        if (*got_frame) {
            printf("video_frame%s n:%d coded_n:%d pts:%" PRId64 "\n",
                   cached ? "(cached)" : "",
                   video_frame_count++, frame->coded_picture_number,
                   frame->pts);

            /* copy decoded frame to destination buffer:
             * this is required since rawvideo expects non aligned data */
            av_image_copy(video_dst_data, video_dst_linesize,
                          (const uint8_t **)(frame->data), frame->linesize,
                          video_dec_ctx->pix_fmt, video_dec_ctx->width, video_dec_ctx->height);

            // write to rawvideo file
            fwrite(video_dst_data[0], 1, video_dst_bufsize, video_dst_file);
        }
    }

    if (*got_frame)
        avcodec_get_frame_defaults(frame);

    return decoded;
}

static int open_codec_context(int *stream_idx,
                              AVFormatContext *fmt_ctx, enum AVMediaType type)
{
    int ret;
    AVStream *st;
    AVCodecContext *dec_ctx = NULL;
    AVCodec *dec = NULL;
    AVDictionary *opts = NULL;

    ret = av_find_best_stream(fmt_ctx, type, -1, -1, NULL, 0);
    if (ret < 0) {
        fprintf(stderr, "Could not find %s stream in input file '%s'\n",
                media_type_string(type), src_filename);
        return ret;
    } else {
        *stream_idx = ret;
        st = fmt_ctx->streams[*stream_idx];

        // find decoder for the stream
        dec_ctx = st->codec;
        dec = avcodec_find_decoder(dec_ctx->codec_id);
        if (!dec) {
            fprintf(stderr, "Failed to find %s codec\n",
                    media_type_string(type));
            return AVERROR(EINVAL);
        }

        // Init the decoders, with or without reference counting
        av_dict_set(&opts, "refcounted_frames", "1", 0);
        if ((ret = avcodec_open2(dec_ctx, dec, &opts)) < 0) {
            fprintf(stderr, "Failed to open %s codec\n",
                    media_type_string(type));
            return ret;
        }
    }

    return 0;
}

int main (int argc, char **argv)
{
    int ret = 0, got_frame;
    AVInputFormat *ifmt = NULL;
    AVDictionary *options = NULL;

    if (argc != 1) {
        fprintf(stderr, "usage: %s input_file video_output_file\n"
                "Records video from the webcam.\n"
                "\n", argv[0]);
        exit(1);
    }
    src_filename = "/dev/video0"; //argv[1];
    video_dst_filename = "video.raw";  //argv[2];

    // register all formats and codecs
    av_register_all();
    avdevice_register_all();

    ifmt = av_find_input_format("video4linux2");
    if (!ifmt) {
        av_log(0, AV_LOG_ERROR, "Cannot find input format\n");
        exit(1);
    }

    fmt_ctx = avformat_alloc_context();
    if (!fmt_ctx)
    {
      av_log(0, AV_LOG_ERROR, "Cannot allocate input format (Out of memory?)\n");
      exit(1);
    }

    // Enable non-blocking mode
    fmt_ctx->flags |= AVFMT_FLAG_NONBLOCK;

    // framerate needs to set before opening the v4l2 device
    av_dict_set(&options, "framerate", "15", 0);
    // This will not work if the camera does not support h264. In that case
    // remove this line. I wrote this for Raspberry Pi where the camera driver
    // can stream h264.
    //av_dict_set(&options, "input_format", "h264", 0);
    av_dict_set(&options, "video_size", "320x224", 0);

    // open input file, and allocate format context
    if (avformat_open_input(&fmt_ctx, src_filename, ifmt, &options) < 0) {
        av_log(0, AV_LOG_ERROR, "Could not open source file %s\n", src_filename);
        exit(1);
    }

    // retrieve stream information
    if (avformat_find_stream_info(fmt_ctx, NULL) < 0) {
        av_log(0, AV_LOG_ERROR, "Could not find stream information\n");
        exit(1);
    }

    if (open_codec_context(&video_stream_idx, fmt_ctx, AVMEDIA_TYPE_VIDEO) >= 0) {
        video_stream = fmt_ctx->streams[video_stream_idx];
        video_dec_ctx = video_stream->codec;

        video_dst_file = fopen(video_dst_filename, "wb");
        if (!video_dst_file) {
            fprintf(stderr, "Could not open destination file %s\n", video_dst_filename);
            ret = 1;
            goto end;
        }

        // allocate image where the decoded image will be put
        ret = av_image_alloc(video_dst_data, video_dst_linesize,
                             video_dec_ctx->width, video_dec_ctx->height,
                             video_dec_ctx->pix_fmt, 1);
        if (ret < 0) {
            fprintf(stderr, "Could not allocate raw video buffer\n");
            goto end;
        }
        video_dst_bufsize = ret;
    }

    // dump input information to stderr
    av_dump_format(fmt_ctx, 0, src_filename, 0);

    if (!video_stream) {
        fprintf(stderr, "Could not find video stream in the input, aborting\n");
        ret = 1;
        goto end;
    }

    frame = avcodec_alloc_frame();
    if (!frame) {
        fprintf(stderr, "Could not allocate frame\n");
        ret = AVERROR(ENOMEM);
        goto end;
    }

    // Set the fields of the given AVFrame to default values
    avcodec_get_frame_defaults(frame);

    // initialize packet, set data to NULL, let the demuxer fill it
    av_init_packet(&pkt);
    pkt.data = NULL;
    pkt.size = 0;

    if (video_stream)
        printf("Demuxing video from file '%s' into '%s'\n", src_filename, video_dst_filename);

    if (video_stream) {
        printf("Play the output video file with the command:\n"
               "avplay -f rawvideo -pixel_format %s -video_size %dx%d %s\n",
               av_get_pix_fmt_name(video_dec_ctx->pix_fmt), video_dec_ctx->width, video_dec_ctx->height,
               video_dst_filename);
    }

    // read frames from the file
    for (size_t i = 0;  i < 100;  ++i) {
        AVPacket orig_pkt;

        ret = av_read_frame(fmt_ctx, &pkt);
        if (ret < 0) {
            if  (ret == AVERROR(EAGAIN)) {
                continue;
            } else {
                break;
            }
        }

        orig_pkt = pkt;
        do {
            ret = decode_packet(&got_frame, 0);
            if (ret < 0)
                break;
            pkt.data += ret;
            pkt.size -= ret;
        } while (pkt.size > 0);
        av_free_packet(&orig_pkt);
    }

    // flush cached frames
    pkt.data = NULL;
    pkt.size = 0;
    do {
        decode_packet(&got_frame, 1);
    } while (got_frame);

    printf("Demuxing succeeded.\n");

end:
    avcodec_close(video_dec_ctx);
    avformat_close_input(&fmt_ctx);
    if (video_dst_file)
        fclose(video_dst_file);
    avcodec_free_frame(&frame);
    av_free(video_dst_data[0]);

    return ret < 0;
}
