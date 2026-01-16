# Binance数据工具 - 核心功能说明

本文档说明项目中三个核心功能的实现位置和使用方法。

## 1. 获取所有存在的币种name list

### 方法一：从AWS S3获取（推荐用于下载场景）

**位置**: `src/bhds/aws/client.py`

**核心方法**: `AwsClient.list_symbols()`

**使用示例**:
```python
from bdt_common.enums import TradeType, DataFrequency, DataType
from bdt_common.network import create_aiohttp_session
from bhds.aws.client import create_aws_client_from_config

async with create_aiohttp_session() as session:
    client = create_aws_client_from_config(
        trade_type=TradeType.spot,
        data_type=DataType.kline,
        data_freq=DataFrequency.daily,
        time_interval="1m",
        session=session,
        http_proxy=None
    )
    symbols = await client.list_symbols()
    print(f"找到 {len(symbols)} 个币种: {symbols[:10]}...")
```

**实现细节**:
- 第109-118行：`list_symbols()` 方法
- 通过 `list_dir()` 方法列出AWS S3目录内容
- 返回排序后的币种名称列表

### 方法二：从本地已下载文件获取

**位置**: `src/bhds/aws/local.py`

**核心方法**: `LocalAwsClient.list_symbols()`

**使用示例**:
```python
from pathlib import Path
from bhds.aws.local import LocalAwsClient
from bhds.aws.path_builder import AwsKlinePathBuilder
from bdt_common.enums import TradeType, DataFrequency

path_builder = AwsKlinePathBuilder(
    trade_type=TradeType.spot,
    data_freq=DataFrequency.daily,
    time_interval="1m"
)
local_client = LocalAwsClient(
    base_dir=Path("~/crypto_data/bhds/aws_data"),
    path_builder=path_builder
)
symbols = local_client.list_symbols()
```

### 方法三：从Binance API获取（包含更多元信息）

**位置**: `src/bdt_common/rest_api/fetcher.py`

**核心方法**: `BinanceFetcher.get_exchange_info()`

**使用示例**:
```python
from bdt_common.enums import TradeType
from bdt_common.rest_api.fetcher import BinanceFetcher
import aiohttp

async with aiohttp.ClientSession() as session:
    fetcher = BinanceFetcher(TradeType.spot, session)
    exchange_info = await fetcher.get_exchange_info()
    symbols = list(exchange_info.keys())
    print(f"从API获取到 {len(symbols)} 个币种")
```

---

## 2. 找到要求的时间、币种所在的URL

### 核心组件

**路径构建器**: `src/bhds/aws/path_builder.py`
- `AwsKlinePathBuilder`: 用于kline数据的路径构建
- `AwsPathBuilder`: 用于其他数据类型的路径构建

**URL构建**: `src/bhds/aws/downloader.py` 第105行
- URL格式: `{BINANCE_AWS_DATA_PREFIX}/{aws_file_path}`
- 常量定义: `src/bdt_common/constants.py`
  - `BINANCE_AWS_DATA_PREFIX = "https://data.binance.vision"`

### 使用流程

**步骤1**: 创建AWS客户端并列出文件

```python
from bdt_common.enums import TradeType, DataFrequency, DataType
from bdt_common.network import create_aiohttp_session
from bhds.aws.client import create_aws_client_from_config

async with create_aiohttp_session() as session:
    client = create_aws_client_from_config(
        trade_type=TradeType.spot,
        data_type=DataType.kline,
        data_freq=DataFrequency.daily,
        time_interval="1m",
        session=session,
        http_proxy=None
    )
    
    # 列出某个币种的所有文件
    symbol = "BTCUSDT"
    files = await client.list_data_files(symbol)
    
    # 文件路径示例: data/spot/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2023-01-01.zip
    for file_path in files[:5]:  # 显示前5个文件
        print(f"文件路径: {file_path}")
```

**步骤2**: 构建完整URL

```python
from bdt_common.constants import BINANCE_AWS_DATA_PREFIX

# 将AWS路径转换为完整URL
for aws_file_path in files:
    full_url = f"{BINANCE_AWS_DATA_PREFIX}/{str(aws_file_path)}"
    print(f"下载URL: {full_url}")
```

**步骤3**: 批量获取多个币种的文件列表

```python
# 批量获取多个币种的文件
symbols = ["BTCUSDT", "ETHUSDT"]
files_map = await client.batch_list_data_files(symbols)

for symbol, files in files_map.items():
    print(f"{symbol}: {len(files)} 个文件")
    for file_path in files[:3]:  # 显示前3个
        url = f"{BINANCE_AWS_DATA_PREFIX}/{str(file_path)}"
        print(f"  {url}")
```

### 文件命名规则

Kline文件的命名格式：
- 日线数据: `{SYMBOL}-{INTERVAL}-{YYYY-MM-DD}.zip`
  - 例如: `BTCUSDT-1m-2023-01-01.zip`
- 月线数据: `{SYMBOL}-{INTERVAL}-{YYYY-MM}.zip`
  - 例如: `BTCUSDT-1m-2023-01.zip`

---

## 3. 下载、解压对应kline的CSV文件

### 3.1 下载功能

**位置**: `src/bhds/aws/downloader.py`

**核心类**: `AwsDownloader`

**主要方法**: `aws_download(aws_files: list[PurePosixPath])`

**使用示例**:
```python
from pathlib import Path
from bhds.aws.downloader import AwsDownloader

# 创建下载器
downloader = AwsDownloader(
    local_dir=Path("~/crypto_data/bhds/aws_data"),
    http_proxy=None,  # 或设置代理
    verbose=True
)

# 下载文件（aws_files是从client.list_data_files()获取的）
downloader.aws_download(aws_files)
```

**实现细节**:
- 第91-144行：`aws_download()` 方法
- 使用 `aria2c` 工具进行多线程下载
- 支持批量下载（每批4096个文件）
- 自动重试机制（默认最多3次）
- 自动跳过已存在的文件

**完整下载流程示例**（参考 `examples/kline_download_task.py`）:

```python
import asyncio
from pathlib import Path
from bdt_common.enums import TradeType, DataFrequency, DataType
from bdt_common.network import create_aiohttp_session
from bhds.aws.client import create_aws_client_from_config
from bhds.aws.downloader import AwsDownloader

async def download_klines():
    async with create_aiohttp_session() as session:
        # 1. 创建客户端
        client = create_aws_client_from_config(
            trade_type=TradeType.spot,
            data_type=DataType.kline,
            data_freq=DataFrequency.daily,
            time_interval="1m",
            session=session,
            http_proxy=None
        )
        
        # 2. 获取币种列表
        symbols = await client.list_symbols()
        target_symbols = symbols[:5]  # 下载前5个币种
        
        # 3. 批量获取文件列表
        files_map = await client.batch_list_data_files(target_symbols)
        
        # 4. 收集所有文件
        from itertools import chain
        all_files = sorted(chain.from_iterable(files_map.values()))
        
        # 5. 下载
        downloader = AwsDownloader(
            local_dir=Path("~/crypto_data/bhds/aws_data"),
            http_proxy=None,
            verbose=True
        )
        downloader.aws_download(all_files)

asyncio.run(download_klines())
```

### 3.2 解压和解析CSV功能

**位置**: `src/bhds/aws/parser.py`

**核心类**: `KlineParser` (继承自 `AwsCsvParser`)

**主要方法**: `read_csv_from_zip(zip_file: Path) -> pl.DataFrame`

**使用示例**:
```python
from pathlib import Path
from bhds.aws.parser import create_aws_parser
from bdt_common.enums import DataType

# 创建解析器
parser = create_aws_parser(DataType.kline)

# 解析ZIP文件中的CSV
zip_file = Path("~/crypto_data/bhds/aws_data/data/spot/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2023-01-01.zip")
df = parser.read_csv_from_zip(zip_file)

print(f"数据行数: {len(df)}")
print(df.head())
print(df.schema)
```

**返回的DataFrame列**:
- `candle_begin_time`: 时间戳（UTC时区）
- `open`: 开盘价
- `high`: 最高价
- `low`: 最低价
- `close`: 收盘价
- `volume`: 成交量
- `quote_volume`: 成交额
- `trade_num`: 成交笔数
- `taker_buy_base_asset_volume`: 主动买入成交量
- `taker_buy_quote_asset_volume`: 主动买入成交额

**实现细节**:
- 第67-101行：`read_csv_from_zip()` 方法
- 自动处理ZIP文件解压
- 自动检测并跳过CSV头部
- 使用Polars进行高效数据处理
- 自动转换时间戳为UTC时区

### 3.3 完整流程：下载 + 验证 + 解析

**参考文件**: `examples/kline_download_task.py`

```python
import asyncio
from pathlib import Path
from bdt_common.enums import TradeType, DataFrequency, DataType
from bdt_common.network import create_aiohttp_session
from bhds.aws.client import create_aws_client_from_config
from bhds.aws.downloader import AwsDownloader
from bhds.aws.checksum import ChecksumVerifier
from bhds.aws.local import AwsDataFileManager
from bhds.aws.parser import create_aws_parser
from itertools import chain

async def download_and_parse():
    async with create_aiohttp_session() as session:
        # 1. 创建客户端
        client = create_aws_client_from_config(
            trade_type=TradeType.spot,
            data_type=DataType.kline,
            data_freq=DataFrequency.daily,
            time_interval="1m",
            session=session,
            http_proxy=None
        )
        
        # 2. 获取币种和文件
        symbols = await client.list_symbols()[:2]  # 前2个币种
        files_map = await client.batch_list_data_files(symbols)
        all_files = sorted(chain.from_iterable(files_map.values()))
        
        # 3. 下载
        data_dir = Path("~/crypto_data/bhds/aws_data")
        downloader = AwsDownloader(local_dir=data_dir, verbose=True)
        downloader.aws_download(all_files)
        
        # 4. 验证校验和
        verifier = ChecksumVerifier(delete_mismatch=False)
        for symbol in symbols:
            symbol_dir = data_dir / str(client.get_symbol_dir(symbol))
            if symbol_dir.exists():
                manager = AwsDataFileManager(symbol_dir)
                unverified = manager.get_unverified_files()
                verifier.verify_files(unverified)
        
        # 5. 解析CSV
        parser = create_aws_parser(DataType.kline)
        for symbol in symbols:
            symbol_dir = data_dir / str(client.get_symbol_dir(symbol))
            manager = AwsDataFileManager(symbol_dir)
            verified_files = manager.get_verified_files()
            
            for zip_file in verified_files[:1]:  # 解析第一个文件
                df = parser.read_csv_from_zip(zip_file)
                print(f"\n{symbol} - {zip_file.name}:")
                print(f"  行数: {len(df)}")
                print(f"  时间范围: {df['candle_begin_time'].min()} 到 {df['candle_begin_time'].max()}")

asyncio.run(download_and_parse())
```

---

## 总结

### 功能1：获取币种列表
- **AWS方式**: `AwsClient.list_symbols()` - 从S3获取
- **本地方式**: `LocalAwsClient.list_symbols()` - 从本地文件获取
- **API方式**: `BinanceFetcher.get_exchange_info()` - 从Binance API获取

### 功能2：找到URL
- **路径构建**: `AwsKlinePathBuilder` / `AwsPathBuilder`
- **文件列表**: `AwsClient.list_data_files()` / `batch_list_data_files()`
- **URL构建**: `f"{BINANCE_AWS_DATA_PREFIX}/{aws_file_path}"`

### 功能3：下载和解压
- **下载**: `AwsDownloader.aws_download()` - 使用aria2c批量下载
- **解压解析**: `KlineParser.read_csv_from_zip()` - 自动解压并解析为DataFrame
- **验证**: `ChecksumVerifier` - 校验文件完整性

### 快速开始示例

完整示例请参考：
- `examples/kline_download_task.py` - 下载任务示例
- `examples/cm_futures_holo.py` - 数据处理示例
