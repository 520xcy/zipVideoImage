#!/usr/bin/python3
# encoding: utf-8

import os
import json
import ffmpy
from concurrent.futures import ProcessPoolExecutor,as_completed
import time
from alive_progress import alive_bar
import random
import argparse
import datetime
import subprocess
import psutil

GB = 1024 ** 3
MB = 1024 ** 2
KB = 1024
MAX_CONNECTIONS = psutil.cpu_count(logical=False)  # 同时转码进程数量
BASHPATH = os.getcwd()
# VIDEO_FORMAT = ['MPEG-4', 'AVI', 'Matroska']
# VIDEO_FORMAT = ['avc', 'msmpeg4v1', 'msmpeg4v2', 'msmpeg4v3', 'mpeg4', '8bps', 'avs', 'bethsoftvid', 'binkvideo', 'bmv_video', 'cdgraphics', 'cdtoons', 'cdxl', 'clearvideo', 'cmv', 'cpia', 'dsicinvideo', 'dvvideo', 'ffv1', 'flic', 'h264', 'hevc', 'hnm4video', 'idcin', 'interplayvideo', 'jv', 'kmvc', 'magicyuv', 'mmvideo', 'motionpixels', 'mpeg1video', 'mpeg2video', 'msvideo1', 'mxpeg', 'paf_video', 'prores', 'qtrle', 'rawvideo', 'rl2', 'roq', 'rpza',
#                'sanm', 'sheervideo', 'smackvideo', 'tgq', 'tgv', 'thp', 'tiertexseqvideo', 'tqi', 'utvideo', 'vmdvideo', 'ws_vqa', 'amv', 'argo', 'cavs', 'flashsv', 'flashsv2', 'flv1', 'gdv', 'indeo4', 'indeo5', 'ipu', 'kgv1', 'mad', 'mobiclip', 'mss2', 'mvc1', 'mvc2', 'nuv', 'prosumer', 'rv10', 'rv20', 'rv30', 'rv40', 'sga', 'simbiosis_imx', 'smvjpeg', 'svq1', 'svq3', 'vc1image', 'vixl', 'vmnc', 'wcmv', 'wmv1', 'wmv2', 'wmv3', 'wmv3image', 'yop', 'zerocodec', 'zmbv']
VIDEO_FORMAT = ['.avi', '.mkv', '.mp4', '.asf', '.mpg', '.mpeg',
                '.mov', '.wmv', '.flv', '.swf', '.m4v', '.ts', '.3gp', '.f4v']
VIDEO_BIT = '2048000'
VIDEO_MAX_WIDTH = 1280
VIDEO_MAX_HEIGHT = 720
IMAGE_WIDTH = 1200
S_INDEX = 0
# IMAGE_FORMAT = ['JPEG', 'Bitmap', 'GIF', 'PNG']
# IMAGE_FORMAT = ['mjpegb', 'adpcm_ima_smjpeg', 'alias_pix', 'apng', 'brender_pi', 'dds', 'dpx', 'exr', 'gem', 'pam', 'pbm', 'pcx', 'pfm', 'pgm',
#                'pgmyuv', 'phm', 'png', 'ppm', 'ptx', 'sgi', 'sunrast', 'targa', 'tiff', 'txd', 'vc1image', 'wmv3image', 'xbm', 'xface', 'xpm', 'xwd', 'mjpeg']
IMAGE_FORMAT = ['.bmp', '.gif', '.png', '.jpg', '.jpeg', '.tif', '.tiff']
PYTHON_NAME = os.path.split(__file__)[-1].split('.')[0]
SUCCESS_LOG = os.path.join(
    BASHPATH, PYTHON_NAME+'-success-'+datetime.datetime.now().strftime('%Y%m%d%H%M%S')+'.txt')
ERROR_LOG = os.path.join(BASHPATH, PYTHON_NAME+'-error-' +
                         datetime.datetime.now().strftime('%Y%m%d%H%M%S')+'.txt')
FFMPEG_CMD = [
    # 硬编硬解
    {
        'inputs': '-y -hwaccel qsv -hwaccel_output_format qsv -c:v h264_qsv -noautorotate',
        'outputs': '-loglevel quiet -vcodec h264_qsv -acodec copy -b:v %s -bufsize %s -vf "scale_qsv=%s" -f mp4'
    },
    {
        'inputs': '-y -hwaccel videotoolbox -hwaccel_output_format videotoolbox -noautorotate',
        'outputs': '-loglevel quiet -vcodec h264_videotoolbox -acodec copy -b:v %s -bufsize %s -vf "scale=%s" -f mp4'
    },
    {
        'inputs': '-y -hwaccel vaapi -hwaccel_output_format vaapi -hwaccel_device /dev/dri/renderD128 -noautorotate',
        'outputs': '-loglevel quiet -vcodec h264_vaapi -acodec copy -b:v %s -bufsize %s -f mp4 -vf "scale_vaapi=%s"'
    },
    {
        'inputs': '-y -hwaccel cuda -hwaccel_output_format cuda -noautorotate',
        'outputs': '-loglevel quiet -vcodec h264_nvenc -acodec copy -b:v %s -bufsize %s -f mp4 -vf "scale_cuda=%s"'
    },
    # 软解硬编
    #{
    #    'inputs': '-y -hwaccel_output_format qsv -noautorotate',
    #    'outputs': '-loglevel quiet -vcodec h264_qsv -acodec copy -b:v %s -bufsize %s -f mp4 -vf "scale=%s"'
    #},
    #{
    #    'inputs': '-y -hwaccel_output_format videotoolbox',
    #    'outputs': '-loglevel quiet -vcodec h264_videotoolbox -acodec copy -b:v %s -bufsize %s -f mp4 -vf "scale=%s"'
    #},
    #{
    #    'inputs': '-y -hwaccel_output_format vaapi',
    #    'outputs': '-loglevel quiet -vcodec h264_vaapi -acodec copy -b:v %s -bufsize %s -f mp4 -vf "scale=%s"'
    #},
    #{
    #    'inputs': '-y -hwaccel_output_format cuda',
    #    'outputs': '-loglevel quiet -vcodec h264_nvenc -acodec copy -b:v %s -bufsize %s -f mp4 -vf "scale=%s"'
    #},
    # 硬解软编
    # {
    #    'inputs': '-y -hwaccel qsv',
    #    'outputs': '-loglevel quiet -b:v %s -c:v libx264 -c:a copy -bufsize %s -f mp4 -vf "scale_qsv=%s"'
    # },
    # {
    #    'inputs': '-y -hwaccel videotoolbox',
    #    'outputs': '-loglevel quiet -b:v %s -c:v libx264 -c:a copy -bufsize %s -f mp4 -vf "scale=%s"'
    # },
    # {
    #    'inputs': '-y -hwaccel vaapi -hwaccel_device /dev/dri/renderD128',
    #    'outputs': '-loglevel quiet -b:v %s -c:v libx264 -c:a copy -bufsize %s -f mp4 -vf "scale=%s"'
    # },
    # {
    #    'inputs': '-y -hwaccel cuda -c:v h264_cuvid',
    #    'outputs': '-loglevel quiet -b:v %s -c:v libx264 -c:a copy -bufsize %s -f mp4 -vf "scale=%s"'
    # },
    # 软解软编
    {
        'inputs': '-y',
        'outputs': '-loglevel quiet -vcodec libx264 -acodec copy -b:v %s -bufsize %s -f mp4 -vf "scale=%s"'
    }
]


def checkFormat(media_info):
    ft = 'unknow'
    ext = os.path.splitext(media_info['format']['filename'])[1].lower()
    if ext in VIDEO_FORMAT:
        for d in media_info['streams']:
            if 'width' in d and 'height' in d:
                if d['width'] > VIDEO_MAX_WIDTH or d['height'] > VIDEO_MAX_WIDTH:
                    if 'bit_rate' in d and int(d['bit_rate']) < int(VIDEO_BIT):
                        break
                    ft = 'video'
                    break
    if ext in IMAGE_FORMAT:
        d = media_info['streams'][0]
        if d['width'] > IMAGE_WIDTH:
            ft = 'image'
    return ft


def getNewName(d):
    file_name = os.path.basename(d)
    folder_name = os.path.dirname(d)
    if not PYTHON_NAME+'_convert_' in str(file_name):
        if not os.path.splitext(str(file_name))[1].lower() == '.mp4' and os.path.isfile(os.path.join(folder_name, os.path.splitext(str(file_name))[0])+'.mp4'):
            return os.path.join(folder_name, PYTHON_NAME+'_convert_'+os.path.splitext(str(file_name))[0]+str(random.randint(0, 1000))+'.mp4')
        return os.path.join(folder_name, PYTHON_NAME+'_convert_'+os.path.splitext(str(file_name))[0]+'.mp4')


def getNewSize(media_info):
    for d in media_info['streams']:
        if 'width' in d and 'height' in d:
            if d["width"] > d["height"]:
                if d["width"] > VIDEO_MAX_WIDTH:
                    return str(VIDEO_MAX_WIDTH)+":trunc(ow/a/16)*16"

                    height = int(VIDEO_MAX_WIDTH*d["height"]/d["width"])
                    height += 16-height % 16
                    return str(VIDEO_MAX_WIDTH)+":"+str(height)
            else:
                if d["height"] > VIDEO_MAX_WIDTH:
                    return "trunc(oh*a/16)*16:" + str(VIDEO_MAX_WIDTH)

                    width = int(VIDEO_MAX_WIDTH*d["width"]/d["height"])
                    width += 16-width % 16
                    return str(width)+":"+str(VIDEO_MAX_WIDTH)


def fileList(path):
    f = []
    for (root, dirs, files) in os.walk(path):
        for name in files:
            f.append(os.path.join(root, name))
        # for name in dirs:
        #     f.append(os.path.join(root, name))
    return f


def writeFile(path, str):
    with open(path, 'a', encoding='utf-8') as f:
        f.write(str)

# 转码


def runFfmpy(src, dst, s):
    o_size = get_size(src)
    for run_type in FFMPEG_CMD:
        try:
            s_time = time.time()
            cmd_src = run_type['inputs']
            cmd_dst = run_type['outputs'] % (VIDEO_BIT, VIDEO_BIT, s)

            ff = ffmpy.FFmpeg(
                inputs={src: cmd_src},
                outputs={dst: cmd_dst}
            )
            print('执行', ff.cmd)
            ff.run()
            e_time = time.time()
            d_size = get_size(dst)
            return o_size, d_size, round(e_time-s_time)
        except ffmpy.FFRuntimeError:
            continue

    raise RuntimeError('未找到合适的解码方式')


def zipVideo(src, dst, size):
    try:
        o_size, d_size, run_time = runFfmpy(
            src, dst, size)
        if d_size < o_size:
            os.remove(src)
            os.rename(dst, dst.replace(
                PYTHON_NAME+'_convert_', ''))
            return {
                'o_size': o_size,
                'd_size': d_size,
                'run_time': run_time
            }

        elif os.path.exists(dst):
            os.remove(dst)
       
    except Exception:
        if os.path.exists(dst):
            os.remove(dst)
        raise

    raise RuntimeError('生成文件大于源文件')

def get_size(file):
    # 获取文件大小:MB
    return os.path.getsize(file)


def get_new_img_name(d):
    file_name = os.path.basename(d)
    folder_name = os.path.dirname(d)
    if not PYTHON_NAME+'_resize_' in str(file_name):
        if not os.path.splitext(str(file_name))[1].lower() == '.jpg' and os.path.isfile(os.path.join(folder_name, os.path.splitext(str(file_name))[0])+'.jpg'):
            return os.path.join(folder_name, PYTHON_NAME+'_resize_'+os.path.splitext(str(file_name))[0]+str(random.randint(0, 1000))+'.jpg')
        return os.path.join(folder_name, PYTHON_NAME+'_resize_'+os.path.splitext(str(file_name))[0]+'.jpg')


def zip_img(infile, outfile):
    o_size = get_size(infile)
    s_time = time.time()
    try:
        ff = ffmpy.FFmpeg(
            inputs={infile: '-y'},
            outputs={outfile: '-loglevel quiet -q 1 -vf "scale=%s:-1"' %
                     (IMAGE_WIDTH)}
        )
        print('执行', ff.cmd)
        ff.run()
        e_time = time.time()
        d_size = get_size(outfile)
        return o_size, d_size, round(e_time-s_time)
    except ffmpy.FFRuntimeError:
        pass

    raise RuntimeError('未找到合适的压缩方式')


def zipImg(filePath, outfile):
    try:
        o_size, d_size, run_time = zip_img(filePath, outfile)
        if d_size < o_size:
            os.remove(filePath)
            os.rename(outfile, outfile.replace(
                PYTHON_NAME+'_resize_', ''))
            return {
                'o_size': o_size,
                'd_size': d_size,
                'run_time': run_time
            }

        elif os.path.exists(outfile):
            os.remove(outfile)
        
    except Exception:
        if os.path.exists(outfile):
            os.remove(outfile)
        raise
    raise RuntimeError('生成文件大于源文件')

def worker(index, file):
    
    try:
        tup_resp = ffmpy.FFprobe(
            inputs={file: None},
            global_options=[
                '-v', 'quiet',
                '-print_format', 'json',
                '-show_streams',
                '-show_format'
            ]
        ).run(stdout=subprocess.PIPE)
        media_info = json.loads(tup_resp[0].decode('utf-8'))
        ft = checkFormat(media_info)
        if ft == 'video' and ziptype != 'image':
            size = getNewSize(media_info)
            dst = getNewName(file)
            if dst and size:
                ret = zipVideo(
                    **{'src': file, 'dst': dst, 'size': size})
                return {
                    'index': index,
                    'filePath': file,
                    **ret
                }
        elif ft == 'image' and ziptype != 'video':
            outfile = get_new_img_name(file)
            if outfile:
                ret = zipImg(file, outfile)
                return {
                    'index': index,
                    'filePath': file,
                    **ret
                }

    except Exception as e:
        raise Exception(f'{index}: {file}:{e}')
    
    raise RuntimeWarning(f'{index}: {file}:跳过')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='自动压缩图片和视频文件')
    parser.add_argument('-t', '--type', type=str, default='',
                        metavar=('video|images'), help='处理类型')
    parser.add_argument('-d', '--dir', type=str, default=os.getcwd(),
                        metavar=(''), help='处理目录的绝对路径')
    parser.add_argument('-vbit', type=str, default=VIDEO_BIT,
                        metavar=(VIDEO_BIT), help='视频码率')
    parser.add_argument('-vmw', type=int, default=VIDEO_MAX_WIDTH,
                        metavar=(VIDEO_MAX_WIDTH), help='视频最长边像素')
    parser.add_argument('-imw', type=int, default=IMAGE_WIDTH,
                        metavar=(IMAGE_WIDTH), help='图片最长边像素')
    parser.add_argument('-s', type=int, default=S_INDEX,
                        metavar=(S_INDEX), help='开始处理文件索引')
    parser.add_argument('-c', type=int, default=MAX_CONNECTIONS,
                        metavar=(MAX_CONNECTIONS), help='线程数量')
    args = parser.parse_args()
    if args.type in ['', 'video', 'image']:
        ziptype = args.type
    else:
        print('t参数为video或image')
        exit()
    path = args.dir
    if not os.path.isdir(path):
        print('所选路径错误')
        exit()
    VIDEO_BIT = args.vbit
    VIDEO_MAX_WIDTH = args.vmw
    IMAGE_WIDTH = args.imw
    S_INDEX = args.s
    MAX_CONNECTIONS = args.c

    files = fileList(path)
    files.sort()
    count = len(files)

    with alive_bar(count) as bar:
        with ProcessPoolExecutor(max_workers=MAX_CONNECTIONS) as executor:
            future_task = []
            for index in range(count):
                if index < S_INDEX - 1:
                    bar()
                    continue
                file = files[index]
                future_task.append(executor.submit(worker, index, file))

            # for f in future_task:
            #     if f.running():
            #         print(f"{str(f)} is running")

            for f in as_completed(future_task):
                try:
                    ret = f.done()
                    if ret:
                        f_ret = f.result()
                        suc = f'{f_ret["index"]}: {f_ret["filePath"]} => Size: {round(f_ret["o_size"] / KB)}kb => {round(f_ret["d_size"] / KB)}kb time{str(f_ret["run_time"])}s'
                        writeFile(SUCCESS_LOG, f'{suc}\n\r')
                        print(suc)
                except RuntimeWarning as e:
                    f.cancel
                    print(e)
                except Exception as e:
                    f.cancel()
                    writeFile(ERROR_LOG, f'{str(e)}\n\r')
                    print(e)
                finally:
                    bar()
