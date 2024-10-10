import os
import typing
from typing import AnyStr, Callable, Union, IO
from urllib.parse import quote

from starlette.background import BackgroundTask
from starlette.responses import FileResponse, Response, StreamingResponse

from pywxdump.common.config.oss_config_manager import OSSConfigManager
from pywxdump.file.Attachment import Attachment
from pywxdump.file.LocalAttachment import LocalAttachment
from pywxdump.file.S3Attachment import S3Attachment


def determine_strategy(file_path: str) -> Attachment:
    """
    根据文件路径确定使用的附件策略（本地或S3）。

    参数:
    file_path (str): 文件路径。

    返回:
    Attachment: 返回对应的附件策略类实例。
    """
    if file_path.startswith(f"s3://"):
        return OSSConfigManager().get_attachment("s3", S3Attachment)
    else:
        return LocalAttachment()


def exists(path: str) -> bool:
    """
    检查文件或目录是否存在。

    参数:
    path (str): 文件或目录路径。

    返回:
    bool: 如果存在返回True，否则返回False。
    """
    return determine_strategy(path).exists(path)


def open(path: str, mode: str) -> IO:
    """
    打开一个文件并返回文件对象。

    参数:
    path (str): 文件路径。
    mode (str): 打开文件的模式。

    返回:
    IO: 文件对象。
    """
    return determine_strategy(path).open(path, mode)


def makedirs(path: str) -> bool:
    """
    创建目录，包括所有中间目录。

    参数:
    path (str): 目录路径。

    返回:
    bool: 总是返回True。
    """
    return determine_strategy(path).makedirs(path)


def join(path: str, *paths: str) -> str:
    """
    连接一个或多个路径组件。

    参数:
    path (str): 第一个路径组件。
    *paths (str): 其他路径组件。

    返回:
    str: 连接后的路径。
    """
    return determine_strategy(path).join(path, *paths)


def dirname(path: str) -> str:
    """
    获取路径的目录名。

    参数:
    path (str): 文件路径。

    返回:
    str: 目录名。
    """
    return determine_strategy(path).dirname(path)


def basename(path: str) -> str:
    """
    获取路径的基本名（文件名）。

    参数:
    path (str): 文件路径。

    返回:
    str: 基本名（文件名）。
    """
    return determine_strategy(path).basename(path)


def send_attachment(
        path: str | os.PathLike[str],
        status_code: int = 200,
        headers: typing.Mapping[str, str] | None = None,
        media_type: str | None = None,
        background: BackgroundTask | None = None,
        filename: str | None = None,
) -> Response:
    """
    发送文件。

    参数:
    path (str): 文件路径。
    status_code (int): HTTP 状态码。
    headers (Mapping[str, str]): HTTP 头。
    media_type (str): MIME 类型。
    background (BackgroundTask): 后台任务。
    filename (str): 文件名。

    返回:
    Response: 响应。
    """
    file_io = open(path, "rb")

    if filename is None:
        filename = basename(path)

    if media_type is None:
        media_type = "application/octet-stream"

    if headers is None:
        headers = {
            "Content-Disposition": f'attachment; filename*=UTF-8\'\'{quote(filename)}',
        }
    else:
        headers = dict(headers)  # 将 Mapping 转换为可变的字典
        headers["Content-Disposition"] = f'attachment; filename*=UTF-8\'\'{quote(filename)}'

    return StreamingResponse(content=file_io, status_code=status_code, headers=headers, media_type=media_type,
                             background=background)


def download_file(db_path, local_path):
    """
    从db_path下载文件到local_path。

    参数:
    db_path (str): 数据库文件路径。
    local_path (str): 本地文件路径。

    返回:
    str: 本地文件路径。
    """
    with open(local_path, 'wb') as f:
        with open(db_path, 'rb') as r:
            f.write(r.read())
    return local_path


def isLocalPath(path: str) -> bool:
    """
    判断路径是否为本地路径。

    参数:
    path (str): 文件或目录路径。

    返回:
    bool: 如果是本地路径返回True，否则返回False。
    """
    strategy = determine_strategy(path)
    return isinstance(strategy, type(LocalAttachment()))


def getsize(path: str):
    """
    获取文件大小

    参数:
    path (str): 文件路径

    返回:
    int: 文件大小
    """
    return determine_strategy(path).getsize(path)
