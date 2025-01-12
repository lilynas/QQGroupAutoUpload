import os
import subprocess
import json
import requests
import time
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import psutil  # 用于检查文件占用
import queue
import threading

# 设置变量
source_dir = os.path.join(os.path.dirname(__file__), "media")
archive_dir = os.path.join(os.path.dirname(__file__), "archive")
uploaded_dir = os.path.join(os.path.dirname(__file__), "uploaded")  # 已上传文件存放目录
log_file = os.path.join(os.path.dirname(__file__), "log.txt")
keyword_dir = os.path.join(os.path.dirname(__file__), "keyword")  # 关键词文件夹
password = "888"
seven_zip_path = r"C:\Program Files\7-Zip\7z.exe"  # 7-Zip 路径
llonebot_api_url = "http://localhost:3000"  # LLOneBot 的 API 地址
group_id = 123123  # 目标 QQ 群号
max_file_size_gb = 3.9  # 最大文件大小（GB）
max_file_size_bytes = max_file_size_gb * 1024 * 1024 * 1024  # 转换为字节

# 创建必要的目录（如果不存在）
if not os.path.exists(archive_dir):
    os.makedirs(archive_dir)
if not os.path.exists(uploaded_dir):
    os.makedirs(uploaded_dir)

# 初始化日志
def log(message):
    # 输出到控制台
    print(message)
    # 写入日志文件（使用 utf-8 编码）
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now()} - {message}\n")

# 检查文件是否被占用
def is_file_locked(file_path):
    for proc in psutil.process_iter(['pid', 'name', 'open_files']):
        try:
            # 检查 proc.info 是否为 None
            if proc.info is None:
                continue
            
            # 检查 open_files 是否为 None
            open_files = proc.info.get('open_files')
            if open_files is None:
                continue
            
            # 检查文件是否被占用
            for file in open_files:
                if file.path == file_path:
                    return True
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
    return False

# 删除文件（带重试逻辑和文件占用检查）
def delete_file_with_retry(file_path, retries=5, delay=2):
    for i in range(retries):
        try:
            if is_file_locked(file_path):
                log(f"Attempt {i + 1}: File is still in use, retrying in {delay} seconds...")
                time.sleep(delay)
                continue
            os.remove(file_path)
            log(f"Deleted: {file_path}")
            return True
        except PermissionError as e:
            log(f"Attempt {i + 1}: File is still in use, retrying in {delay} seconds...")
            time.sleep(delay)
    log(f"Failed to delete: {file_path} after {retries} attempts")
    return False

# 加载关键词
def load_keywords(keyword_dir):
    keywords = set()  # 使用集合避免重复关键词
    if not os.path.exists(keyword_dir):
        log(f"Warning: Keyword directory '{keyword_dir}' does not exist. No keywords will be loaded.")
        return list(keywords)

    # 遍历 keyword 文件夹中的所有 .txt 文件
    for filename in os.listdir(keyword_dir):
        if filename.endswith(".txt"):
            file_path = os.path.join(keyword_dir, filename)
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    for line in f:
                        keyword = line.strip()  # 去除空白字符
                        if keyword:  # 忽略空行
                            keywords.add(keyword)
                log(f"Loaded keywords from file: {filename}")
            except Exception as e:
                log(f"Error loading keywords from file {filename}: {e}")

    log(f"Total keywords loaded: {len(keywords)}")  # 只记录关键词数量
    return list(keywords)

# 文件名关键词替换
def replace_keywords(filename, keywords):
    for keyword in keywords:
        filename = filename.replace(keyword, "")  # 替换关键词为空字符串
    return filename

# 移动文件到 uploaded 文件夹
def move_file_to_uploaded(file_path):
    try:
        file_name = os.path.basename(file_path)
        new_file_path = os.path.join(uploaded_dir, file_name)
        # 移动文件
        os.rename(file_path, new_file_path)
        log(f"Moved file to uploaded folder: {file_path} -> {new_file_path}")
    except Exception as e:
        log(f"Error moving file to uploaded folder: {e}")

# 检查文件是否已完全写入
def is_file_fully_written(file_path, check_interval=10, max_attempts=6):
    """
    检查文件是否已完全写入。
    :param file_path: 文件路径
    :param check_interval: 检查间隔（秒）
    :param max_attempts: 最大检查次数
    :return: True（文件已完全写入），False（文件仍在写入）
    """
    previous_size = -1
    for attempt in range(max_attempts):
        current_size = os.path.getsize(file_path)
        if current_size == previous_size:
            log(f"File is fully written: {file_path} (size: {current_size} bytes)")
            # 确认文件完成写入后，等待 10 秒，确保文件占用被解除
            log("Waiting for 10 seconds to ensure file is released...")
            time.sleep(10)
            return True
        previous_size = current_size
        log(f"File is still being written: {file_path} (size: {current_size} bytes), waiting {check_interval} seconds...")
        time.sleep(check_interval)
    log(f"File may still be incomplete: {file_path} (size: {previous_size} bytes)")
    return False

# 压缩、上传和移动文件的逻辑
def process_file(file_path, keywords):
    file_name = os.path.basename(file_path)
    # 替换文件名中的关键词
    cleaned_file_name = replace_keywords(file_name, keywords)
    archive_file = os.path.join(archive_dir, f"{os.path.splitext(cleaned_file_name)[0]}.zip")

    # 检查文件是否已完全写入
    if not is_file_fully_written(file_path):
        log(f"Skipped: {file_name} is still being written.")
        return  # 跳过未完全写入的文件

    # 检查文件大小是否超过 3.9GB
    file_size = os.path.getsize(file_path)
    if file_size > max_file_size_bytes:
        log(f"Skipped: {file_name} is larger than {max_file_size_gb}GB ({file_size / (1024 * 1024 * 1024):.2f}GB)")
        return  # 跳过大于 3.9GB 的文件

    # 检查压缩包是否已经存在
    if os.path.exists(archive_file):
        log(f"Skipped: {file_name} is already compressed as {archive_file}")
        return  # 跳过已压缩的文件

    # 使用 7-Zip 进行压缩
    try:
        command = [
            seven_zip_path, "a", "-tzip", archive_file, file_path,
            f"-p{password}", "-bsp1"
        ]
        result = subprocess.run(command, capture_output=True, text=True)

        if result.returncode == 0:
            log(f"Successfully compressed: {file_name} to {archive_file}")

            # 上传压缩文件到 QQ 群
            try:
                # 构建上传请求
                url = f"{llonebot_api_url}/upload_group_file"
                payload = json.dumps({
                    "group_id": group_id,
                    "file": archive_file,
                    "name": os.path.basename(archive_file)
                })
                headers = {
                    'Content-Type': 'application/json'
                }

                # 发送上传请求
                response = requests.post(url, headers=headers, data=payload)
                
                # 检查响应是否为空
                if response is None:
                    raise Exception("API response is None. Check network connection or API server status.")
                
                # 尝试解析响应为 JSON
                try:
                    response_data = response.json()
                except json.JSONDecodeError:
                    raise Exception(f"Invalid API response (not JSON): {response.text}")

                # 检查上传结果
                if response.status_code == 200 and response_data.get("retcode") == 0:
                    log(f"Successfully uploaded: {archive_file} to QQ group {group_id}")

                    # 上传成功后等待 10 秒，确保文件被释放
                    log("Waiting for 10 seconds to ensure file is released...")
                    time.sleep(10)

                    # 删除压缩文件（带重试逻辑）
                    if delete_file_with_retry(archive_file):
                        log(f"Deleted: {archive_file}")
                    else:
                        log(f"Failed to delete: {archive_file}")

                    # 移动原视频文件到 uploaded 文件夹
                    move_file_to_uploaded(file_path)
                else:
                    raise Exception(f"Upload failed: {response.text}")
            except Exception as e:
                log(f"Error during upload: {e}")
        else:
            raise Exception(f"Compression failed: {result.stderr}")
    except Exception as e:
        log(f"Error: {e}")

# 任务队列
task_queue = queue.Queue()

# 任务处理线程
def task_processor(keywords):
    while True:
        file_path = task_queue.get()  # 从队列中获取任务
        if file_path is None:  # 退出信号
            break
        try:
            # 第一个任务也等待 10 秒
            if task_queue.unfinished_tasks == 1:
                log("Waiting for 10 seconds before processing the first task...")
                time.sleep(10)
            process_file(file_path, keywords)
        except Exception as e:
            log(f"Error processing file {file_path}: {e}")
        finally:
            task_queue.task_done()  # 标记任务完成
            time.sleep(10)  # 每个任务之间间隔 10 秒

# 监控 media 文件夹的变动
class MediaFolderHandler(FileSystemEventHandler):
    def __init__(self, keywords):
        self.keywords = keywords  # 关键词列表

    def on_created(self, event):
        if not event.is_directory:  # 只处理文件，忽略文件夹
            file_path = event.src_path
            if file_path.lower().endswith((".mp4", ".avi", ".mkv", ".mov")):
                log(f"New file detected: {file_path}")
                task_queue.put(file_path)  # 将文件路径加入任务队列

# 启动监控
if __name__ == "__main__":
    log("=========================================")
    log("Script started")
    log(f"Monitoring folder: {source_dir}")

    # 加载关键词
    keywords = load_keywords(keyword_dir)

    # 启动时扫描 media 文件夹中的现有文件
    for file_name in os.listdir(source_dir):
        file_path = os.path.join(source_dir, file_name)
        if os.path.isfile(file_path) and file_path.lower().endswith((".mp4", ".avi", ".mkv", ".mov")):
            log(f"Existing file detected: {file_path}")
            task_queue.put(file_path)  # 将文件路径加入任务队列

    # 启动任务处理线程
    processor_thread = threading.Thread(target=task_processor, args=(keywords,))
    processor_thread.daemon = True  # 设置为守护线程
    processor_thread.start()

    # 初始化监控器
    event_handler = MediaFolderHandler(keywords)
    observer = Observer()
    observer.schedule(event_handler, path=source_dir, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)  # 保持脚本运行
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

    # 等待任务队列中的任务完成
    task_queue.join()
    log("Script ended")
    log("=========================================")
