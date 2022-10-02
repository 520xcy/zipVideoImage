# encoding: utf-8

from email import message
from genericpath import isdir
import os
import json
import sys
import ffmpy
from pymediainfo import MediaInfo
import threading
import time
from PIL import Image
from alive_progress import alive_bar
import random
import argparse

MAX_CONNECTIONS = 2  # 同时转码进程数量
BASHPATH = os.getcwd()
VIDEO_FORMAT = ['MPEG-4', 'AVI', 'Matroska', 'Windows Media']
IMAGE_FORMAT = ['JPEG', 'Bitmap', 'GIF', 'PNG']
LOG_FILE = str(time.time())
FILE_NAME = os.path.split(__file__)[-1].split('.')[0]
SUCCESS_LOG = os.path.join(
    BASHPATH, FILE_NAME+'-success-'+str(time.time())+'.txt')
ERROR_LOG = os.path.join(BASHPATH, FILE_NAME+'-error-'+str(time.time())+'.txt')
FFMPEG_CMD = {
    # 硬编硬解
    'hdhe': {
        'darwin': {
            'inputs': '-y -hwaccel videotoolbox',
            'outputs': '-loglevel quiet -b:v 2000k -c:v h264_videotoolbox -acodec copy -bufsize 2000k -f mp4 -vf scale=%s'
        },
        'linux': {
            'inputs': '-y -hwaccel vaapi -hwaccel_output_format vaapi -hwaccel_device /dev/dri/renderD128',
            'outputs': '-loglevel quiet -b:v 2000k -c:v h264_vaapi -acodec copy -bufsize 2000k -f mp4 -vf scale_vaapi=%s'
        },
        'win32': {
            'inputs': '-y -hwaccel cuda -c:v h264_cuvid -hwaccel_output_format cuda',
            'outputs': '-loglevel quiet -b:v 2000k -c:v h264_nvenc -acodec copy -bufsize 2000k -f mp4 -vf scale_cuda=%s'
        }
    },
    # 软解硬编
    'sdhe': {
        'darwin': {
            'inputs': '-y',
            'outputs': '-loglevel quiet -b:v 2000k -c:v h264_videotoolbox -acodec copy -bufsize 2000k -f mp4 -vf scale=%s'
        },
        'linux': {
            'inputs': '-y -hwaccel_output_format vaapi',
            'outputs': '-loglevel quiet -b:v 2000k -c:v h264_vaapi -acodec copy -bufsize 2000k -f mp4 -vf scale=%s'
        },
        'win32': {
            'inputs': '-y -hwaccel_output_format cuda',
            'outputs': '-loglevel quiet -b:v 2000k -c:v h264_nvenc -acodec copy -bufsize 2000k -f mp4 -vf scale=%s'
        }
    },
    # 硬解软编
    'hdse': {
        'darwin': {
            'inputs': '-y -hwaccel videotoolbox',
            'outputs': '-loglevel quiet -b:v 2000k -c:v libx264 -acodec copy -bufsize 2000k -f mp4 -vf scale=%s'
        },
        'linux': {
            'inputs': '-y -hwaccel vaapi -hwaccel_device /dev/dri/renderD128',
            'outputs': '-loglevel quiet -b:v 2000k -c:v libx264 -acodec copy -bufsize 2000k -f mp4 -vf scale=%s'
        },
        'win32': {
            'inputs': '-y -hwaccel cuda -c:v h264_cuvid',
            'outputs': '-loglevel quiet -b:v 2000k -c:v libx264 -acodec copy -bufsize 2000k -f mp4 -vf scale=%s'
        }
    },
    # 软解软编
    'sdse': {
        'darwin': {
            'inputs': '-y',
            'outputs': '-loglevel quiet -b:v 2000k -c:v libx264 -acodec copy -bufsize 2000k -f mp4 -vf scale=%s'
        },
        'linux': {
            'inputs': '-y',
            'outputs': '-loglevel quiet -b:v 2000k -c:v libx264 -acodec copy -bufsize 2000k -f mp4 -vf scale=%s'
        },
        'win32': {
            'inputs': '-y',
            'outputs': '-loglevel quiet -b:v 2000k -c:v libx264 -acodec copy -bufsize 2000k -f mp4 -vf scale=%s'
        }
    }
}


def checkVideoFormat(d):
    format = False
    video = False
    for f in d['tracks']:
        if f['track_type'] == 'General':
            if 'format' in f and f['format'] in VIDEO_FORMAT:
                format = True
        if f['track_type'] == 'Video':
            if (f['height'] > 720 and f['width'] > 1280) or (f['width'] > 720 and f['height'] > 1280):
                video = True
    return format and video

def getNewVideoName(d):
    for f in d['tracks']:
        if f['track_type'] == 'General':
            if not FILE_NAME+'_convert_' in f['file_name_extension']:
                return os.path.join(f['folder_name'], FILE_NAME+'_convert_'+f['file_name_extension'])
            else:
                if os.path.isfile(os.path.join(f['folder_name'], f['file_name']+'.mp4')):
                    return os.path.join(f['folder_name'], FILE_NAME+'_convert_'+f['file_name']+str(random.randint(0, 1000))+'.mp4')
                return os.path.join(f['folder_name'], FILE_NAME+'_convert_'+f['file_name']+'.mp4')

def getNewSize(d):
    for f in d['tracks']:
        if f['track_type'] == 'Video':
            if f["width"] > f["height"]:
                return '1280:-1'
            else:
                return '-1:1280'


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


def runFfmpy(src, dst, s='-1:720'):
    for run_type in FFMPEG_CMD:
        try:
            s_time = time.time()
            ff = ffmpy.FFmpeg(
                inputs={src: FFMPEG_CMD[run_type][sys.platform]['inputs']},
                outputs={dst: FFMPEG_CMD[run_type]
                         [sys.platform]['outputs'] % s}
            )
            print('执行', ff.cmd)
            ff.run()
            e_time = time.time()
            return run_type, e_time-s_time
        except:
            continue

    raise RuntimeError('未找到合适的解码方式')


class zipVideo(threading.Thread):
    tlist = []  # 用来存储队列的线程
    # int(sys.argv[2])最大的并发数量，此处我设置为100，测试下系统最大支持1000多个
    maxthreads = MAX_CONNECTIONS
    evnt = threading.Event()  # 用事件来让超过最大线程设置的并发程序等待
    lck = threading.Lock()  # 线程锁

    def __init__(self, src, dst, size):
        threading.Thread.__init__(self)
        self.src = src
        self.dst = dst
        self.size = size

    def run(self):
        try:
            run_type, run_time = runFfmpy(self.src, self.dst, self.size)
            writeFile(SUCCESS_LOG, f'{run_type} {self.src} {run_time}\n\r')
            os.remove(self.src)
            os.rename(self.dst, os.path.splitext(self.src)[0]+'.mp4')
        except Exception as e:
            if os.path.exists(self.dst):
                os.remove(self.dst)
            print('发生错误:', self.src, e)
            writeFile(ERROR_LOG,
                      f'{self.src}:{str(e)}\n\r')
            pass

        # 以下用来将完成的线程移除线程队列
        self.lck.acquire()
        self.tlist.remove(self)
        # 如果移除此完成的队列线程数刚好达到99，则说明有线程在等待执行，那么我们释放event，让等待事件执行
        if len(self.tlist) == self.maxthreads-1:
            self.evnt.set()
            self.evnt.clear()
        self.lck.release()

    def newthread(src, dst, size):
        zipVideo.lck.acquire()  # 上锁
        sc = zipVideo(src, dst, size)
        zipVideo.tlist.append(sc)
        zipVideo.lck.release()  # 解锁
        sc.start()
    # 将新线程方法定义为静态变量，供调用
    newthread = staticmethod(newthread)


def checkImageFormat(d):
    for f in d['tracks']:
        if f['track_type'] == 'General':
            return 'format' in f and f['format'] in IMAGE_FORMAT


def get_size(file):
    # 获取文件大小:KB
    size = os.path.getsize(file)
    return size / 1024


def get_new_img_name(d):
    for f in d['tracks']:
        if f['track_type'] == 'General':
            if f['format'] == 'JPEG' and not FILE_NAME+'_resize_' in f['file_name_extension']:
                return os.path.join(f['folder_name'], FILE_NAME+'_resize_'+f['file_name_extension'])
            else:
                if os.path.isfile(os.path.join(f['folder_name'], f['file_name']+'.jpg')):
                    return os.path.join(f['folder_name'], FILE_NAME+'_resize_'+f['file_name']+str(random.randint(0, 1000))+'.jpg')
                return os.path.join(f['folder_name'], FILE_NAME+'_resize_'+f['file_name']+'.jpg')


def zip_img(infile, outfile, x_s=1800, kb=1024, step=10, quality=90):
    o_size = get_size(infile)
    with Image.open(infile) as im:
        x, y = im.size
        if x <= x_s:
            return outfile, o_size, o_size
        y_s = int(y * x_s / x)
        im = im.convert('RGB')
        im.resize((x_s, y_s))
        im.save(outfile, quality=100)
        while get_size(outfile) > kb:
            im.save(outfile, quality=quality)
            if quality - step <= 0:
                break
            quality -= step
        os.remove(infile)
        os.rename(outfile, outfile.replace(FILE_NAME+'resize_', ''))
        d_size = get_size(outfile.replace(FILE_NAME+'resize_', ''))
        return outfile, o_size, d_size


class zipImg(threading.Thread):
    tlist = []  # 用来存储队列的线程
    # int(sys.argv[2])最大的并发数量，此处我设置为100，测试下系统最大支持1000多个
    maxthreads = MAX_CONNECTIONS
    evnt = threading.Event()  # 用事件来让超过最大线程设置的并发程序等待
    lck = threading.Lock()  # 线程锁

    def __init__(self, filePath, outfile):
        threading.Thread.__init__(self)
        self.filePath = filePath
        self.outfile = outfile

    def run(self):
        try:
            d_filePath, o_size, d_size = zip_img(self.filePath, self.outfile)
            if o_size != d_size:
                writeFile(SUCCESS_LOG,
                          f'{self.filePath} => {d_filePath} Size: {int(o_size)}kb => {int(d_size)}kb\n\r')
        except Exception as e:
            writeFile(ERROR_LOG, f'{self.filePath}: {str(e)}\n\r')
            pass

        # 以下用来将完成的线程移除线程队列
        self.lck.acquire()
        self.tlist.remove(self)
        # 如果移除此完成的队列线程数刚好达到99，则说明有线程在等待执行，那么我们释放event，让等待事件执行
        if len(self.tlist) == self.maxthreads-1:
            self.evnt.set()
            self.evnt.clear()
        self.lck.release()

    def newthread(filePath, outfile):
        zipImg.lck.acquire()  # 上锁
        sc = zipImg(filePath, outfile)
        zipImg.tlist.append(sc)
        zipImg.lck.release()  # 解锁
        sc.start()
    # 将新线程方法定义为静态变量，供调用
    newthread = staticmethod(newthread)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='自动压缩图片和视频文件')
    parser.add_argument('-t', type=str, default='',
                        metavar=('video|images'), help='处理类型')
    parser.add_argument('-d', type=str, default=os.getcwd(),
                        metavar=(''), help='处理目录的绝对路径')
    args = parser.parse_args()
    if args.t in ['', 'video', 'image']:
        ziptype = args.t
    else:
        print('t参数为video或image')
        exit()
    path = args.d
    if not os.path.isdir(path):
        print('所选路径错误')
        exit()

    pool_sema = threading.Semaphore(MAX_CONNECTIONS*2)
    try:
        files = fileList(path)
        count = len(files)
        start = time.perf_counter()
        with alive_bar(len(files)) as bar:
            for file in files:
                bar()
                media_info = MediaInfo.parse(file)
                data = json.loads(media_info.to_json())
                if checkVideoFormat(data) and ziptype != 'image':
                    size = getNewSize(data)
                    dst = getNewVideoName(data)
                    if not dst:continue
                    zipVideo.lck.acquire()
                    if len(zipVideo.tlist) >= zipVideo.maxthreads:
                        zipVideo.lck.release()
                        zipVideo.evnt.wait()  # zipVideo.evnt.set()遇到set事件则等待结束
                    else:
                        zipVideo.lck.release()
                        zipVideo.newthread(
                            **{'src': file, 'dst': dst, 'size': size})
                elif checkImageFormat(data) and ziptype != 'video':

                    outfile = get_new_img_name(data)
                    if not outfile: continue
                    zipImg.lck.acquire()
                    # 如果目前线程队列超过了设定的上线则等待。
                    if len(zipImg.tlist) >= zipImg.maxthreads:
                        zipImg.lck.release()
                        zipImg.evnt.wait()  # zipImg.evnt.set()遇到set事件则等待结束
                    else:
                        zipImg.lck.release()
                    zipImg.newthread(file, outfile)

        for list in (zipVideo.tlist + zipImg.tlist):
            list.join()
    except KeyboardInterrupt:
        exit()
