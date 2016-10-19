/** audio_sensor.cc                                                   -*- C++ -*-
    Jeremy Barnes, 23 September 2016
    Copyright (c) 2016 Datacratic Inc.  All rights reserved.

    Audio sensor for MLDB, based upon the ffmpeg / libav APIs.  
*/

extern "C" {

#include <libavutil/imgutils.h>
#include <libavutil/samplefmt.h>
#include <libavutil/opt.h>
#include <libavcodec/avcodec.h>
#include <libavformat/avformat.h>
#include <libavdevice/avdevice.h>

} // extern "C"


#include "av_plugin.h"
#include "mldb/core/sensor.h"
#include "mldb/types/value_description.h"
#include "mldb/types/structure_description.h"
#include "mldb/base/scope.h"
#include "mldb/types/any_impl.h"
#include "mldb/http/http_exception.h"
#include "mldb/types/date.h"
#include "mldb/sql/expression_value.h"
#include <iostream>
#include <thread>
#include <chrono>

using namespace std;

namespace MLDB {

struct CaptureAudioSensorConfig {
    std::string driver = "alsa";
    std::string device = "hw:0";
};

DECLARE_STRUCTURE_DESCRIPTION(CaptureAudioSensorConfig);
DEFINE_STRUCTURE_DESCRIPTION(CaptureAudioSensorConfig);

CaptureAudioSensorConfigDescription::
CaptureAudioSensorConfigDescription()
{
    nullAccepted = true;
}


struct CaptureAudioSensor: public Sensor {
    /**
     * Copied from libav/cmdutils.c because unlike FFmpeg, Libav does not export
     * this function in the public API.
     */
    static const char *media_type_string(enum AVMediaType media_type)
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
                    media_type_string(type), "filename");
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

            const AVCodecDescriptor *desc
                = avcodec_descriptor_get(dec_ctx->codec_id);
            cerr << "codec is " << desc->name << " " << desc->long_name << endl;

#if 0
            const AVOption * opt
                = av_opt_next(dec_ctx, nullptr);

            while (opt) {
                cerr << "codec option " << opt->name << " " << opt->help
                     << " " << opt->flags << endl;
                fprintf(stderr, "codec option %s %s\n",
                        opt->name, opt->help);
                opt = av_opt_next(dec_ctx, opt);
            }
       
            cerr << "done options" << endl;
#endif     

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

    static int decode_packet(AVPacket & pkt, int streamIndex,
                             AVCodecContext * audio_dec_ctx,
                             AVFrame * frame,
                             int *got_frame, int cached)
    {
        //frame->nb_samples = 65536;

        int ret = 0;
        int decoded = pkt.size;

        *got_frame = 0;

        //cerr << "pkt.size = " << pkt.size << endl;

        if (pkt.stream_index == streamIndex) {
            // decode audio frame
            ret = avcodec_decode_audio4(audio_dec_ctx, frame, got_frame, &pkt);
            if (ret < 0) {
                // FFmpeg users should use av_err2str
                char errbuf[128];
                av_strerror(ret, errbuf, sizeof(errbuf));
                fprintf(stderr, "Error decoding audio frame (%s)\n", errbuf);
                cerr << "frame had " << pkt.size << " bytes" << endl;
                return ret;
            }

            if (*got_frame) {
#if 1
                cerr << "got frame " << frame->coded_picture_number << " " 
                     << frame->pts << endl;
                cerr << "frame->nb_samples = " << frame->nb_samples << endl;
                cerr << "frame->linesize[0] = " << frame->linesize[0] << endl;
                cerr << "frame->sample_rate = " << frame->sample_rate << endl;
                cerr << "frame->channel_layout = " << frame->channel_layout << endl;
                //cerr << "frame->extended_data = " << frame->extended_data << endl;
                //cerr << "frame->data[0] = " << string(frame->data[0],
                //                                      frame->data[0] + frame->linesize[0]) << endl;
                //cerr << "frame->extended_data[0] = " << frame->extended_data[0] << endl;
                cerr << "frame->format = " << frame->format << endl;
                //printf("audio_frame%s n:%d coded_n:%d pts:%" PRId64 "\n",
                //       cached ? "(cached)" : "",
                //       audio_frame_count++, frame->coded_picture_number,
                //       frame->pts);
             
                //int16_t * dataPtr = (int16_t *)(frame->data);
                int16_t * dataPtr = (int16_t *)(frame->data[0]);
                cerr << dataPtr[0] << " " << dataPtr[1] << " " << dataPtr[2]
                     << " " << dataPtr[3] << endl;
   
#endif
            }
        }

        return decoded;
    }


    CaptureAudioSensor(MldbServer * server,
                       const PolyConfig & pconfig,
                       std::function<bool (Json::Value)> onProgress)
        : Sensor(server)
    {
        std::ios::sync_with_stdio(true);

        config = pconfig.params.convert<CaptureAudioSensorConfig>();

        AVInputFormat * ifmt = av_find_input_format(config.driver.c_str());

        if (!ifmt) {
            av_log(0, AV_LOG_ERROR, "Cannot find input format\n");
            throw HttpReturnException(500, "Cannot find alsa device");
        }


        fmt_ctx = avformat_alloc_context();
        if (!fmt_ctx) {
            av_log(0, AV_LOG_ERROR, "Cannot allocate input format (Out of memory?)\n");
            throw HttpReturnException(500, "Cannot allocate context");
        }

        // Enable non-blocking mode
        fmt_ctx->flags |= AVFMT_FLAG_NONBLOCK;

        AVDictionary *options = nullptr;

        // framerate needs to set before opening the v4l2 device
        //av_dict_set(&options, "framerate", "15", 0);
        // This will not work if the camera does not support h264. In that case
        // remove this line. I wrote this for Raspberry Pi where the camera driver
        // can stream h264.
        //av_dict_set(&options, "input_format", "pcm_u888le", 0);
        //av_dict_set(&options, "audio_size", "320x224", 0);

        // open input file, and allocate format context
        if (avformat_open_input(&fmt_ctx, config.device.c_str(), ifmt, &options) < 0) {
            av_log(0, AV_LOG_ERROR, "Could not open source file %s\n", config.device.c_str());
            exit(1);
        }

#if 0        
        const AVOption * opt
            = av_opt_next(fmt_ctx, nullptr);

        while (opt) {
            cerr << "codec option " << opt->name << " " << opt->help
                 << " " << opt->flags << endl;
            opt = av_opt_next(fmt_ctx, opt);
            cerr << "next option is " << opt << endl;
        }
        
        cerr << "done options" << endl;
#endif


        AVDictionaryEntry *e = nullptr;
        if ((e = av_dict_get(options, "", NULL, AV_DICT_IGNORE_SUFFIX))) {
            cerr << "option " << e->key << " = " << e->value << " is not recognsed by the codec" << endl;
        }

        // retrieve stream information
        if (avformat_find_stream_info(fmt_ctx, NULL) < 0) {
            av_log(0, AV_LOG_ERROR, "Could not find stream information\n");
            exit(1);
        }

#if 0
        cerr << "doing input formats" << endl;

        AVInputFormat * fmt = nullptr;
        while ((fmt = av_iformat_next(fmt))) {
            cerr << "input format " << fmt->name << " " << fmt->long_name << endl;
        }

        cerr << "done input formats" << endl;
#endif

        av_dump_format(fmt_ctx, 0, "audio stream", 0);

        if (open_codec_context(&audio_stream_idx, fmt_ctx, AVMEDIA_TYPE_AUDIO) < 0) {
            throw HttpReturnException(500, "Couldn't open audio codec");
        }

        cerr << "audio is in stream " << audio_stream_idx << endl;
    }

    ~CaptureAudioSensor()
    {
        avformat_free_context(fmt_ctx);

    }

    int audio_stream_idx = -1;
    AVFormatContext * fmt_ctx = nullptr;

    virtual ExpressionValue latest()
    {
        int ret = 0;

        AVStream * audio_stream = fmt_ctx->streams[audio_stream_idx];
        AVCodecContext * audio_dec_ctx = audio_stream->codec;

        cerr << "bit rate is " << audio_dec_ctx->sample_rate << endl;
        cerr << "channels " << audio_dec_ctx->channels << endl;
        cerr << "sample_fmt = " << av_get_sample_fmt_name(audio_dec_ctx->sample_fmt)
             << endl;
        cerr << "planar = " << av_sample_fmt_is_planar(audio_dec_ctx->sample_fmt);

        size_t bytesPerSample = av_get_bytes_per_sample(audio_dec_ctx->sample_fmt);

        auto decodeOnePacked = [&] (const uint8_t * & start)
            {
                float result;
                switch (audio_dec_ctx->sample_fmt) {
                case AV_SAMPLE_FMT_U8:
                case AV_SAMPLE_FMT_U8P:         // unsigned 8 bits, planar
                    result = *(uint8_t *)(start);
                    start += 1;
                    break;
                case AV_SAMPLE_FMT_S16:         // signed 16 bits
                case AV_SAMPLE_FMT_S16P:        // signed 16 bits, planar
                    result = *(uint16_t *)(start);
                    start += 2;
                    break;
                case AV_SAMPLE_FMT_S32:         // signed 32 bits
                case AV_SAMPLE_FMT_S32P:        // signed 32 bits, planar
                    result = *(uint32_t *)(start);
                    start += 4;
                    break;
                case AV_SAMPLE_FMT_FLT:         // float
                case AV_SAMPLE_FMT_FLTP:        // float, planar
                    result = *(float *)(start);
                    start += 4;
                    break;
                case AV_SAMPLE_FMT_DBL:
                case AV_SAMPLE_FMT_DBLP:
                    result = *(double *)(start);
                    start += 8;
                    break;
                case AV_SAMPLE_FMT_NONE:
                case AV_SAMPLE_FMT_NB:
                    throw HttpReturnException(500, "Unknown audio sample format");
                }
                return result;
            };

        auto decodeSample = [&] (const uint8_t * & start,
                                 size_t numBytes)
            {
                size_t numSamples = numBytes / bytesPerSample;

                std::vector<std::vector<float> > result(audio_dec_ctx->channels);
                for (auto & r: result)
                    r.resize(numSamples);

                for (size_t i = 0;  i < numSamples;  ++i) {


                }
                
            };



        AVFrame * frame = avcodec_alloc_frame();
        if (!frame) {
            fprintf(stderr, "Could not allocate frame\n");
            ret = AVERROR(ENOMEM);
            throw HttpReturnException(600, "Couldn't allocate frame");
        }

        // Set the fields of the given AVFrame to default values
        avcodec_get_frame_defaults(frame);
        frame->nb_samples = 65536;

        int got_frame = 0;

        size_t numSamples = 0;

        Date before = Date::now();

        for (;;) {
            AVPacket pkt;
            av_init_packet(&pkt);
            pkt.data = NULL;
            pkt.size = 0;

            cerr << "reading" << endl;
            ret = av_read_frame(fmt_ctx, &pkt);
            if (ret < 0) {
                if  (ret == AVERROR(EAGAIN)) {
                    cerr << "sleeping after " << numSamples << " samples at "
                         << numSamples / before.secondsUntil(Date::now()) << endl;
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                    continue;
                } else {
                    char buf[1024];
                    int n = av_strerror(ret, buf, 1024);
                    if (n < 0) {
                        cerr << "can't get error" << endl;
                    }
                    else {
                        cerr << string(buf, buf + n) << endl;
                    }
                    break;
                }
            }

            // Copy this one so that we can free the right thing at the end
            AVPacket orig_pkt = pkt;

            do {
                int ret = 0;
                int decoded = pkt.size;

                *got_frame = 0;

                //cerr << "pkt.size = " << pkt.size << endl;
                ExcAssertEqual(pkt.stream_index, streamIndex);
                ret = avcodec_decode_audio4(audio_dec_ctx, frame, got_frame, &pkt);
                if (ret < 0) {
                    // FFmpeg users should use av_err2str
                    char errbuf[128];
                    av_strerror(ret, errbuf, sizeof(errbuf));
                    fprintf(stderr, "Error decoding audio frame (%s)\n", errbuf);
                    cerr << "frame had " << pkt.size << " bytes" << endl;
                    return ret;
                }

                if (!got_frame)
                    break;

                int16_t * dataPtr = (int16_t *)(frame->data[0]);
                vector<float> output;
                

#if 1
                        cerr << "got frame " << frame->coded_picture_number << " " 
                             << frame->pts << endl;
                        cerr << "frame->nb_samples = " << frame->nb_samples << endl;
                        cerr << "frame->linesize[0] = " << frame->linesize[0] << endl;
                        cerr << "frame->sample_rate = " << frame->sample_rate << endl;
                        cerr << "frame->channel_layout = " << frame->channel_layout << endl;
                        //cerr << "frame->extended_data = " << frame->extended_data << endl;
                        //cerr << "frame->data[0] = " << string(frame->data[0],
                        //                                      frame->data[0] + frame->linesize[0]) << endl;
                        //cerr << "frame->extended_data[0] = " << frame->extended_data[0] << endl;
                        cerr << "frame->format = " << frame->format << endl;
                        //printf("audio_frame%s n:%d coded_n:%d pts:%" PRId64 "\n",
                        //       cached ? "(cached)" : "",
                        //       audio_frame_count++, frame->coded_picture_number,
                        //       frame->pts);
             
                        //int16_t * dataPtr = (int16_t *)(frame->data);
                        cerr << dataPtr[0] << " " << dataPtr[1] << " " << dataPtr[2]
                             << " " << dataPtr[3] << endl;
   
#endif
                    }
                

                ret = decode_packet(pkt, audio_stream_idx, audio_dec_ctx, frame,
                                    &got_frame, 0);

                if (ret < 0)
                    break;

                if (got_frame) {
                    numSamples += frame->nb_samples;
                    avcodec_get_frame_defaults(frame);
                }

                //cerr << "decoded " << ret << endl;
                pkt.data += ret;
                pkt.size -= ret;
            } while (pkt.size > 0);
            av_free_packet(&orig_pkt);
        }



        std::vector<float> result;
        result.resize(2);
        return ExpressionValue(std::move(result), Date::now());
    }
    
    virtual std::shared_ptr<ExpressionValueInfo> resultInfo() const
    {
        return std::make_shared<EmbeddingValueInfo>(2, ST_FLOAT32);
    }

    CaptureAudioSensorConfig config;
};

static RegisterSensorType<CaptureAudioSensor, CaptureAudioSensorConfig>
regAudioSensor(avPackage(),
               "av.capture.audio",
               "Audio capture sensor for microphones/webcams/USB audio sources",
               "CaptureAudioSensor.md.html");


} // namespace MLDB

#if 0
extern "C"
{
#include <avcodec.h>
#include <avformat.h>
#include <swscale.h>
};

int main()
{
	// Initialize FFmpeg
	av_register_all();

	AVFrame* frame = avcodec_alloc_frame();
	if (!frame)
	{
		std::cout << "Error allocating the frame" << std::endl;
		return 1;
	}

	// you can change the file name "01 Push Me to the Floor.wav" to whatever the file is you're reading, like "myFile.ogg" or
	// "someFile.webm" and this should still work
	AVFormatContext* formatContext = NULL;
	if (avformat_open_input(&formatContext, "01 Push Me to the Floor.wav", NULL, NULL) != 0)
	{
		av_free(frame);
		std::cout << "Error opening the file" << std::endl;
		return 1;
	}

	if (avformat_find_stream_info(formatContext, NULL) < 0)
	{
		av_free(frame);
		av_close_input_file(formatContext);
		std::cout << "Error finding the stream info" << std::endl;
		return 1;
	}

	AVStream* audioStream = NULL;
	// Find the audio stream (some container files can have multiple streams in them)
	for (unsigned int i = 0; i < formatContext->nb_streams; ++i)
	{
		if (formatContext->streams[i]->codec->codec_type == AVMEDIA_TYPE_AUDIO)
		{
			audioStream = formatContext->streams[i];
			break;
		}
	}

	if (audioStream == NULL)
	{
		av_free(frame);
		av_close_input_file(formatContext);
		std::cout << "Could not find any audio stream in the file" << std::endl;
		return 1;
	}

	AVCodecContext* codecContext = audioStream->codec;

	codecContext->codec = avcodec_find_decoder(codecContext->codec_id);
	if (codecContext->codec == NULL)
	{
		av_free(frame);
		av_close_input_file(formatContext);
		std::cout << "Couldn't find a proper decoder" << std::endl;
		return 1;
	}
	else if (avcodec_open2(codecContext, codecContext->codec, NULL) != 0)
	{
		av_free(frame);
		av_close_input_file(formatContext);
		std::cout << "Couldn't open the context with the decoder" << std::endl;
		return 1;
	}

	std::cout << "This stream has " << codecContext->channels << " channels and a sample rate of " << codecContext->sample_rate << "Hz" << std::endl;
	std::cout << "The data is in the format " << av_get_sample_fmt_name(codecContext->sample_fmt) << std::endl;

	AVPacket packet;
	av_init_packet(&packet);

	// Read the packets in a loop
	while (av_read_frame(formatContext, &packet) == 0)
	{
		if (packet.stream_index == audioStream->index)
		{
			// Try to decode the packet into a frame
			int frameFinished = 0;
			avcodec_decode_audio4(codecContext, frame, &frameFinished, &packet);

			// Some frames rely on multiple packets, so we have to make sure the frame is finished before
			// we can use it
			if (frameFinished)
			{
				// frame now has usable audio data in it. How it's stored in the frame depends on the format of
				// the audio. If it's packed audio, all the data will be in frame->data[0]. If it's in planar format,
				// the data will be in frame->data and possibly frame->extended_data. Look at frame->data, frame->nb_samples,
				// frame->linesize, and other related fields on the FFmpeg docs. I don't know how you're actually using
				// the audio data, so I won't add any junk here that might confuse you. Typically, if I want to find
				// documentation on an FFmpeg structure or function, I just type "<name> doxygen" into google (like
				// "AVFrame doxygen" for AVFrame's docs)
			}
		}

		// You *must* call av_free_packet() after each call to av_read_frame() or else you'll leak memory
		av_free_packet(&packet);
	}

	// Some codecs will cause frames to be buffered up in the decoding process. If the CODEC_CAP_DELAY flag
	// is set, there can be buffered up frames that need to be flushed, so we'll do that
	if (codecContext->codec->capabilities & CODEC_CAP_DELAY)
	{
		av_init_packet(&packet);
		// Decode all the remaining frames in the buffer, until the end is reached
		int frameFinished = 0;
		while (avcodec_decode_audio4(codecContext, frame, &frameFinished, &packet) >= 0 && frameFinished)
		{
		}
	}

	// Clean up!
	av_free(frame);
	avcodec_close(codecContext);
	av_close_input_file(formatContext);
}
#endif
