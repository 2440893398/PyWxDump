import concurrent.futures
import os
import shutil
import tempfile
import time

from crcmod import crcmod
from qcloud_cos import CosConfig, CosS3Client
from pywxdump import decrypt_merge
from tqdm import tqdm



# 解密微信数据库
def decrypt_wechat_db(key, db_path, output_path):
    code, merge_save_path = decrypt_merge(db_path, key, output_path, is_merge_data= True)
    if code:
        # 删除合并前的解密数据库文件
        out_path = os.path.join(output_path, "decrypted")
        # if os.path.exists(out_path):
        #     try:
        #         shutil.rmtree(out_path)
        #     except PermissionError as e:
        #         raise e
        # return merge_save_path
    else:
        raise Exception(merge_save_path)



# 上传文件到腾讯云COS
def upload_to_cos(secret_id, secret_key, region, bucket, local_file_path, remote_key):
    config = CosConfig(Region=region, SecretId=secret_id, SecretKey=secret_key, Token=None, Scheme='https')
    client = CosS3Client(config)

    # Query if the object exists in COS
    try:
        response = client.head_object(
            Bucket=bucket,
            Key=remote_key
        )
        # Get the remote file ETag (MD5 hash)
        remote_md5 = response['x-cos-hash-crc64ecma']
        if remote_md5:
            crc64 = calculate_crc64(local_file_path)
            if crc64 == remote_md5:
                return
    except Exception:
        pass
    with open(local_file_path, 'rb') as fp:
        response = client.put_object(
            Bucket=bucket,
            Body=fp,
            Key=remote_key,
            StorageClass='STANDARD',
            EnableMD5=False
        )
    return response


def calculate_crc64(file_path):
    if not os.path.isfile(file_path):
        raise ValueError(f"{file_path} is not a valid file path.")

    # 创建 CRC 函数
    c64_func = crcmod.mkCrcFun(0x142F0E1EBA9EA3693, initCrc=0, xorOut=0xffffffffffffffff, rev=True)
    c64 = 0  # 初始化 CRC64 值

    # 读取文件并更新 CRC64 值
    try:
        with open(file_path, 'rb') as file:
            chunk = file.read(8192)  # 读取文件内容
            while chunk:
                c64 = c64_func(chunk, c64)  # 更新 CRC64 值
                chunk = file.read(8192)  # 继续读取下一块内容
    except IOError as e:
        print(f"Error reading file {file_path}: {e}")
        return None
    return str(c64)


# 清理解密后的本地文件
def clean_decrypted_files(output_path):
    if os.path.exists(output_path):
        shutil.rmtree(output_path)

# 主函数
def main():
    wechat_id = "wxid_fhded1nyrrdr22"
    wechat_secret_key = 'd1aec8085bc14d6da0ded5c2a8ce6ee3057b90551f56476aaf98278f22db14af'
    wechat_db_path = 'C:\\Users\\24408\\Documents\\WeChat Files\\wxid_fhded1nyrrdr22'


    # wechat_id = "wxid_rbdv29hm552s22"
    # wechat_secret_key = '759fd87ca9cd4b3cba596628ad20cdf464657e40a2554a9881682e73ffbf1c94'
    # wechat_db_path = 'C:\\Users\\24408\\Documents\\WeChat Files\\wxid_rbdv29hm552s22'
    # 将解密后的文件保存到临时目录
    decrypted_output_dir = tempfile.gettempdir()+os.sep+'wechat_decrypted_files'
    if not os.path.exists(decrypted_output_dir):
        os.makedirs(decrypted_output_dir)
    print("Decrypted files will be saved to:", decrypted_output_dir)
    cos_secret_id = "AKIDaAjAh5JZmuwupiaTKAxfI1kR4gFdv67v"
    cos_secret_key = "wlT2ldSBkQBsXCh077va9e4Qh4fEev47"
    cos_region = 'ap-nanjing'
    cos_bucket = 'wechatmsg-1256220500'

    # Step 1: 解密微信数据库
    decrypt_wechat_db(wechat_secret_key, wechat_db_path, decrypted_output_dir)
    time.sleep(1)
    # Step 2: 将解密后的结果上传到腾讯云COS
    # for root, dirs, files in os.walk(decrypted_output_dir):
    #     for file in files:
    #         local_file_path = os.path.join(root, file)
    #         relative_path = os.path.relpath(local_file_path, decrypted_output_dir)
    #         # 上传到COS的文件路径 = wechat_secret_key + 相对路径
    #         remote_key = os.path.join(wechat_id, relative_path).replace(os.sep, '/')
    #         upload_to_cos(cos_secret_id, cos_secret_key, cos_region, cos_bucket, local_file_path, remote_key)
    # # Step 3: 清理解密后的本地文件
    # clean_decrypted_files(decrypted_output_dir)
    # # Step 4: 同步附件文件忽略Cache、Temp、TempFromPhone目录
    # ignore_directories = ['Cache', 'Temp', 'TempFromPhone']
    # wechat_attachment_dir = os.path.join(wechat_db_path, 'FileStorage')
    #
    # # 上传到COS的文件路径 = wechat_secret_key + 相对路径，并发上传
    # with concurrent.futures.ProcessPoolExecutor() as executor:
    #     futures = []
    #     # 获取所有文件
    #     for root, dirs, files in os.walk(wechat_attachment_dir):
    #         # 忽略指定的目录
    #         dirs[:] = [d for d in dirs if d not in ignore_directories]
    #         # 收集文件路径 忽略空目录
    #         if not files:
    #             continue
    #         for file in tqdm(files, desc=f'目录：{root}文件同步', unit='file',mininterval=1):
    #             local_file_path = os.path.join(root, file)
    #             relative_path = os.path.relpath(local_file_path, wechat_db_path)
    #             remote_key = os.path.join(wechat_id, relative_path).replace(os.sep, '/')
    #             futures.append(
    #                 executor.submit(upload_to_cos(cos_secret_id, cos_secret_key, cos_region, cos_bucket, local_file_path, remote_key))
    #             )
    # # 等待所有任务完成
    # for future in concurrent.futures.as_completed(futures):
    #     try:
    #         future.result()
    #     except Exception as e:
    #         print(f"Error during upload: {e}")

if __name__ == '__main__':
    main()
