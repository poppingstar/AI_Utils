import PIL.Image as Image
import PIL.ImageFile as ImageFile
from pathlib import Path
import threading, os, piexif, shutil
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
ImageFile.LOAD_TRUNCATED_IMAGES=True


def multithreading(func, input_list, *args, **kwrgs):
    def wrapper():
        cpu_cores = os.cpu_count() or 1

        chunks = list_equal_split(input_list, cpu_cores)

        results = []
        with ThreadPoolExecutor(cpu_cores) as executor:
            for chunk in chunks:
                future = executor.submit(func, chunk, *args, **kwrgs)
                results.append(future.result())
        return results

    return wrapper()


def multiprocessing(func, input_list, *args, **kwrgs):
    def wrapper():
        cpu_cores = os.cpu_count() or 1

        chunks = list_equal_split(input_list, cpu_cores)

        results = []
        with ProcessPoolExecutor(cpu_cores) as executor:
            for chunk in chunks:
                future = executor.submit(func, chunk, *args, **kwrgs)
                results.append(future.result())
        return results

    return wrapper()


def list_equal_split(input_list, chunk_num):
    list_len = len(input_list)
    chunk_num = chunk_num if (chunk_num != 0) and (chunk_num <= list_len) else list_len
    chunk_size, residual = divmod(list_len, chunk_num)
    
    chunks = []
    start = 0
    for _ in range(chunk_num):
        end = start + chunk_size

        if residual > 0:
            end += 1
            residual -= 1

        chunk = input_list[start:end]
        chunks.append(chunk)
        start = end

    return chunks


def is_rgb(img_path:Path):
    with Image.open(img_path) as img:
        return img.mode == 'RGB'


def separate_non_rgb(directory_list:list, root:Path):
    separation_dir = root/'non_rgb'
    for directory in directory_list:
        directory = Path(directory)
        for file in directory.iterdir():
            if is_rgb(file):
                continue
            
            separation_dir.mkdir(exist_ok=True)
            name = file.stem
            extension = file.suffix
            file_path = separation_dir/directory/file.name

            i=1
            while file_path.exists():
                file_path = separation_dir/directory/f'{name}({i}){extension}'
                i+=1
            file.rename(file_path)


def get_files(p:Path):
    if p.is_file():
        return [p]
    
    files = []
    for child in p.iterdir():
        if child.is_dir():
            files.extend(get_files(child))
        elif child.is_file():
            files.append(child)
    
    return files


def dataset_split(input_dir:Path|str, val_rate:float, test_rate:float):
    input_dir = Path(input_dir)
    assert val_rate + test_rate <= 1, '합계 비율이 1 이하여야 합니다'
    assert input_dir.is_dir(), '입력및 출력 디렉토리는 반드시 디렉토리여야합니다'

    for sub_dir in input_dir.iterdir():
        files = list(sub_dir.iterdir())
        file_num = len(files)
        val_num = int(file_num * val_rate)
        test_num = int(file_num * test_rate)
        
        val_files = files[:val_num]
        test_files = files[val_num:val_num+test_num]
        train_files = files[val_num+test_num:]

        for file_subset, group in ((val_files, 'valid'), (test_files, 'test'), (train_files, 'train')):
            current_dir = input_dir/group/sub_dir.name  
            current_dir.mkdir(exist_ok=True, parents=True)
            for file in file_subset:
                shutil.move(file, current_dir/file.name)
        
        if sub_dir.name not in ('train', 'test', 'valid'):
            os.rmdir(sub_dir)


def chk_corrupt(root:Path, dirlist):
    separtion_dir = root/'corrupt_img'

    for sub_dir in dirlist:
        sub_dir = Path(sub_dir)
        for file in sub_dir.iterdir():
            try:
                with Image.open(file) as img:
                    img.verify()
                    exif_data = img.info.get('exif')
                    if exif_data:
                        piexif.load(exif_data)
            except:
                sepration_sub_dir = separtion_dir/sub_dir.name
                sepration_sub_dir.mkdir(exist_ok=True, parents=True)
                new_path = sepration_sub_dir/file.name
                
                file.rename(new_path)


if __name__ == '__main__':
    root = Path(r"E:\refined")
    dataset_split(root, 0.2,0.1)

    
def main(root):
    cpu_num = os.cpu_count()

    train = root/'train'
    valid = root/'valid'
    # test = root/'test'
    for d in (train, valid):
        sub_dirs = [sub for sub in d.iterdir()]
        chunk = len(sub_dirs) // cpu_num

        residual = len(sub_dirs) % chunk
        threads = []
        start=0
        for i in range(cpu_num):
            end=start+chunk
            if residual>i:
                end+=1
            dir_per_thread = sub_dirs[start:end]
            start=end

            # thread = threading.Thread(target=separate_non_rgb, args=(lst, root))
            thread = threading.Thread(target=chk_corrupt, args=(root, dir_per_thread))
            threads.append(thread)
            thread.start()
 
        for thread in threads:
            thread.join()
