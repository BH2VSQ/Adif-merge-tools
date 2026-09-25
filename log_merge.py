import os
import re
import shutil
import sys
import gc
from collections import defaultdict
from datetime import datetime


def get_base_path():
    """
    获取脚本或 EXE 的实际运行路径。
    """
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


def get_qso_time(rec):
    """
    从记录中提取 datetime 对象。
    合并 QSO_DATE 和 TIME_ON。
    """
    date_str = rec.get('QSO_DATE', '')

    # TIME_ON 可能是 4 位或 6 位，统一补全为 6 位。
    time_str = rec.get('TIME_ON', '000000')
    if len(time_str) < 6:
        time_str = time_str.ljust(6, '0')
    else:
        time_str = time_str[:6]

    try:
        return datetime.strptime(
            f"{date_str}{time_str}",
            "%Y%m%d%H%M%S"
        )
    except (TypeError, ValueError):
        return None


def normalize_grid(raw_grid):
    """
    将 Maidenhead Grid Square 统一为前四位。

    例如：
        PN11QW -> PN11
        PN11RS -> PN11
        pn11ab -> PN11

    这里返回的值同时用于：
    1. 输出文件分组
    2. 写回 ADIF 的 MY_GRIDSQUARE
    """
    if raw_grid is None:
        return None

    grid = str(raw_grid).strip().upper()

    if not grid:
        return None

    # 仅保留字母和数字，避免特殊字符影响文件名。
    grid = ''.join(c for c in grid if c.isalnum())

    if len(grid) >= 4:
        return grid[:4]

    # 长度不足四位时，保留清洗后的原值。
    return grid


class AdifParser:
    """
    流式 ADIF 解析器，避免一次性读取大文件导致内存压力。

    使用二进制模式读取，因为 ADIF 长度标签表示的是字节长度。
    读取后优先按 UTF-8 解码，失败后使用 GB18030。
    """

    def __init__(self, file_path):
        self.file_path = file_path
        self.file_name = os.path.basename(file_path)

        # ADIF 标签支持：
        # <TAG:length>
        # <TAG:length:type>
        self.tag_pattern = re.compile(
            rb'<([^:>]+):(\d+)(?::[^>]+)?>',
            re.IGNORECASE
        )

    def _parse_single_record(self, raw_data):
        """
        解析单条 ADIF 字节流为字典。
        """
        if not raw_data.strip():
            return None

        record_data = {
            '_SOURCE_FILE': self.file_name
        }

        pos = 0
        data_len = len(raw_data)

        while pos < data_len:
            tag_match = self.tag_pattern.search(raw_data, pos)

            if not tag_match:
                break

            # 标签名为 ASCII。
            tag_name = tag_match.group(1).decode(
                'ascii',
                errors='ignore'
            ).upper()

            value_len = int(tag_match.group(2))

            value_start = tag_match.end()
            value_end = value_start + value_len

            if value_end <= data_len:
                value_bytes = raw_data[value_start:value_end]

                # 优先 UTF-8。
                try:
                    value_str = value_bytes.decode('utf-8')
                except UnicodeDecodeError:
                    # 兼容 GBK / GB2312 等常见中文日志。
                    try:
                        value_str = value_bytes.decode('gb18030')
                    except UnicodeDecodeError:
                        value_str = value_bytes.decode(
                            'utf-8',
                            errors='replace'
                        )

                record_data[tag_name] = value_str
                pos = value_end
            else:
                # 标签声明的长度超出当前记录剩余数据。
                break

        if len(record_data) > 1:
            return record_data

        return None

    def stream_records(self):
        """
        生成器：逐条读取 ADIF 记录。

        使用缓冲区处理跨块记录，避免一次性加载整个文件。
        """
        buffer = b''
        chunk_size = 1024 * 1024 * 2  # 2 MB

        try:
            with open(self.file_path, 'rb') as f:
                header_found = False

                while True:
                    chunk = f.read(chunk_size)

                    if not chunk:
                        break

                    buffer += chunk

                    # 尝试寻找 <EOH>。
                    if not header_found:
                        lower_buf = buffer.lower()
                        eoh_idx = lower_buf.find(b'<eoh>')

                        if eoh_idx != -1:
                            buffer = buffer[eoh_idx + 5:]
                            header_found = True
                        else:
                            # 如果长时间找不到 Header，
                            # 允许继续尝试解析，以兼容无 Header 文件。
                            if len(buffer) > 10 * 1024 * 1024:
                                header_found = True
                            else:
                                continue

                    # 处理 <EOR> 分隔的完整记录。
                    while True:
                        lower_buf = buffer.lower()
                        eor_idx = lower_buf.find(b'<eor>')

                        if eor_idx == -1:
                            break

                        raw_rec = buffer[:eor_idx]
                        buffer = buffer[eor_idx + 5:]

                        parsed_rec = self._parse_single_record(raw_rec)

                        if parsed_rec:
                            yield parsed_rec

                # 文件末尾可能存在没有 <EOR> 的记录。
                if buffer.strip():
                    parsed_rec = self._parse_single_record(buffer)

                    if parsed_rec:
                        yield parsed_rec

        except Exception as e:
            print(f"读取文件 {self.file_name} 时出错: {e}")


class FastDeduplicator:
    """
    高效去重管理器。

    在同一个：
        STATION_CALLSIGN + 4位 MY_GRIDSQUARE

    分组内，根据：
        CALL + BAND + MODE + 15分钟时间窗口

    判断重复记录。
    """

    def __init__(self):
        # Key=GroupKey
        # Value=[Records]
        self.final_records = defaultdict(list)

        # Key=GroupKey
        # -> {(DX_Call, Band, Mode): [(TimeObj, Record)]}
        self.lookup_index = defaultdict(
            lambda: defaultdict(list)
        )

        self.dupe_details = []

    def process_record(self, record, group_key):
        """
        处理单条记录。
        """
        dx_call = record.get('CALL', '').upper()
        band = record.get('BAND', '').upper()
        mode = record.get('MODE', '').upper()
        qso_time = get_qso_time(record)

        # 缺少关键字段时，直接保留，不参与严格去重。
        if not dx_call or not qso_time:
            self._add_to_storage(
                group_key,
                record,
                dx_call,
                band,
                mode,
                qso_time
            )
            return

        # 只有相同 CALL + BAND + MODE 的记录才需要进一步比对时间。
        key = (dx_call, band, mode)
        candidates = self.lookup_index[group_key][key]

        is_dupe = False
        existing_rec = None

        for exist_time, exist_rec in candidates:
            diff = abs(
                (qso_time - exist_time).total_seconds()
            )

            # 15 分钟以内视为重复。
            if diff <= 900:
                is_dupe = True
                existing_rec = exist_rec
                break

        if is_dupe:
            self.dupe_details.append({
                'station': group_key,
                'new_rec': record,
                'old_rec': existing_rec
            })
        else:
            self._add_to_storage(
                group_key,
                record,
                dx_call,
                band,
                mode,
                qso_time
            )

    def _add_to_storage(
        self,
        group_key,
        record,
        dx_call,
        band,
        mode,
        qso_time
    ):
        """
        将记录加入最终存储和查重索引。
        """
        self.final_records[group_key].append(record)

        if dx_call and qso_time:
            key = (dx_call, band, mode)
            self.lookup_index[group_key][key].append(
                (qso_time, record)
            )


def write_adif_file(file_path, records):
    """
    写入 ADIF 文件。
    """
    header = (
        "ADIF Export from Python Tool (Splitted by Call-4CharGrid)\r\n"
        "Created by Adif-merge-tools\r\n"
        "<ADIF_VER:5>3.1.4\r\n"
        "<EOH>\r\n"
    )

    try:
        # 使用 GB18030，兼容中文日志软件。
        out_encoding = 'gb18030'

        with open(
            file_path,
            'w',
            encoding=out_encoding,
            newline=''
        ) as f:
            f.write(header)

            for rec in records:
                line = ""

                for tag, val in rec.items():
                    if tag.startswith('_'):
                        continue

                    val_str = str(val)

                    # ADIF 长度以字节计算，而不是字符数。
                    value_len = len(
                        val_str.encode(out_encoding)
                    )

                    line += (
                        f"<{tag}:{value_len}>"
                        f"{val_str} "
                    )

                f.write(line + "<EOR>\r\n")

    except Exception as e:
        print(f"写入文件 {file_path} 失败: {e}")


def generate_html_report(report_path, dupe_details):
    """
    生成重复记录 HTML 报告。
    """
    if not dupe_details:
        return

    # 防止重复记录过多导致 HTML 文件过大。
    MAX_REPORT_ITEMS = 5000
    display_items = dupe_details[:MAX_REPORT_ITEMS]

    html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>ADIF Deduplication Visual Report</title>
    <style>
        body {{
            font-family: sans-serif;
            background: #f4f7f6;
            padding: 20px;
        }}

        h1 {{
            color: #2c3e50;
        }}

        .summary {{
            background: white;
            padding: 15px;
            border-radius: 8px;
            margin-bottom: 20px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
        }}

        table {{
            width: 100%;
            border-collapse: collapse;
            background: white;
            box-shadow: 0 2px 15px rgba(0,0,0,0.1);
        }}

        th,
        td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }}

        th {{
            background-color: #34495e;
            color: white;
        }}

        tr:hover {{
            background-color: #f1f1f1;
        }}

        .diff-container {{
            display: flex;
            gap: 10px;
            font-size: 0.85em;
        }}

        .rec-box {{
            background: #ebf5fb;
            padding: 10px;
            border-radius: 4px;
            border: 1px solid #aed6f1;
            flex: 1;
        }}

        .existing {{
            background: #fef9e7;
            border-color: #f9e79f;
        }}

        .tag {{
            font-weight: bold;
            color: #7f8c8d;
        }}

        .val {{
            color: #2980b9;
        }}

        .file-info {{
            display: block;
            margin-bottom: 8px;
            font-weight: bold;
            color: #2c3e50;
            border-bottom: 1px dashed #ccc;
            padding-bottom: 4px;
        }}

        .warning {{
            color: red;
            font-weight: bold;
            margin-top: 10px;
        }}
    </style>
</head>
<body>
    <h1>ADIF 查重比对报告</h1>

    <div class="summary">
        <p>
            <strong>生成时间:</strong>
            {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        </p>

        <p>
            <strong>重复条目总数:</strong>
            {len(dupe_details)}
        </p>

        {
            f'<p class="warning">注意：报告仅显示前 '
            f'{MAX_REPORT_ITEMS} 条，以免文件过大。</p>'
            if len(dupe_details) > MAX_REPORT_ITEMS
            else ''
        }
    </div>

    <table>
        <thead>
            <tr>
                <th>序号</th>
                <th>电台呼号-4位网格</th>
                <th>比对详情（重复项 vs 原始项）</th>
            </tr>
        </thead>
        <tbody>
"""

    for i, detail in enumerate(display_items, 1):
        new_rec = detail['new_rec']
        old_rec = detail['old_rec']

        def format_rec(record):
            if not record:
                return "无法读取记录"

            return (
                f"<span class='tag'>CALL:</span> "
                f"<span class='val'>{record.get('CALL', '')}</span> | "
                f"<span class='tag'>BAND:</span> "
                f"<span class='val'>{record.get('BAND', '')}</span> | "
                f"<span class='tag'>GRID:</span> "
                f"<span class='val'>"
                f"{record.get('MY_GRIDSQUARE', '')}"
                f"</span><br>"
                f"<span class='tag'>TIME:</span> "
                f"<span class='val'>"
                f"{record.get('QSO_DATE', '')} "
                f"{record.get('TIME_ON', '')}"
                f"</span>"
            )

        html_content += f"""
            <tr>
                <td>{i}</td>
                <td><strong>{detail['station']}</strong></td>
                <td>
                    <div class="diff-container">
                        <div class="rec-box">
                            <span class="file-info">
                                重复项来源:
                                {new_rec.get('_SOURCE_FILE', '未知')}
                            </span>
                            {format_rec(new_rec)}
                        </div>

                        <div class="rec-box existing">
                            <span class="file-info">
                                原始项来源:
                                {old_rec.get('_SOURCE_FILE', '未知')}
                            </span>
                            {format_rec(old_rec)}
                        </div>
                    </div>
                </td>
            </tr>
"""

    html_content += """
        </tbody>
    </table>
</body>
</html>
"""

    with open(
        report_path,
        'w',
        encoding='utf-8'
    ) as f:
        f.write(html_content)


def process_adi_files():
    """
    主处理流程：

    1. 扫描当前目录及 done 目录中的 ADIF 文件
    2. 流式读取 QSO
    3. 将 MY_GRIDSQUARE 统一为前四位
    4. 按 STATION_CALLSIGN + 四位网格分组
    5. 分组内进行重复 QSO 检查
    6. 导出合并后的 ADIF 文件
    7. 生成重复报告
    8. 将当前目录中的原始 ADIF 归档到 done
    """
    current_dir = get_base_path()
    output_dir = os.path.join(
        current_dir,
        'output'
    )
    done_dir = os.path.join(
        current_dir,
        'done'
    )

    # 1. 准备目录。
    if os.path.exists(output_dir):
        print(f"正在清理输出目录: {output_dir}")
        shutil.rmtree(output_dir)

    os.makedirs(output_dir)

    if not os.path.exists(done_dir):
        os.makedirs(done_dir)

    # 初始化去重器。
    deduplicator = FastDeduplicator()
    unknown_sources = defaultdict(int)

    # 2. 扫描文件。
    valid_exts = ('.adi', '.adif')

    root_files = [
        f for f in os.listdir(current_dir)
        if f.lower().endswith(valid_exts)
    ]

    done_files = [
        os.path.join('done', f)
        for f in os.listdir(done_dir)
        if f.lower().endswith(valid_exts)
    ]

    all_target_files = root_files + done_files

    if not all_target_files:
        print(
            f"错误: 在 {current_dir} 及其 done 目录中"
            f"未找到 ADIF 文件 (.adi/.adif)。"
        )
        input("按回车键退出...")
        return

    print(
        f"找到 {len(all_target_files)} 个文件。"
        f"开始分析 (按 呼号-4位网格 拆分)..."
    )
    print("-" * 50)

    # 3. 处理文件。
    total_qso_count = 0
    file_count = 0

    for file_path_rel in all_target_files:
        file_count += 1

        full_path = os.path.join(
            current_dir,
            file_path_rel
        )

        print(
            f"[{file_count}/{len(all_target_files)}] "
            f"正在读取: {file_path_rel}"
        )

        parser = AdifParser(full_path)
        rec_count = 0

        # 流式读取。
        for rec in parser.stream_records():
            rec_count += 1

            # -------------------------------------------------
            # 核心逻辑：
            # 统一 MY_GRIDSQUARE 为 Maidenhead 前四位。
            #
            # 例如：
            #     PN11QW -> PN11
            #     PN11RS -> PN11
            #
            # 同时修改 rec 中的 MY_GRIDSQUARE，
            # 因此最终输出 ADIF 也是四位网格。
            # -------------------------------------------------

            raw_callsign = rec.get('STATION_CALLSIGN')
            raw_grid = rec.get('MY_GRIDSQUARE')

            # 处理呼号。
            if not raw_callsign or not str(raw_callsign).strip():
                call_part = 'UNKNOWN'
                unknown_sources[file_path_rel] += 1
            else:
                call_part = (
                    str(raw_callsign)
                    .strip()
                    .upper()
                    .replace('/', '_')
                )

            # 处理网格。
            grid_part = normalize_grid(raw_grid)

            if grid_part:
                # 直接修改输出记录中的 MY_GRIDSQUARE。
                rec['MY_GRIDSQUARE'] = grid_part

                # 使用四位网格建立分组。
                group_key = (
                    f"{call_part}-{grid_part}"
                )
            else:
                # 无 MY_GRIDSQUARE 时进入 NOGRID 分组。
                group_key = (
                    f"{call_part}-NOGRID"
                )

            # 交给去重器。
            deduplicator.process_record(
                rec,
                group_key
            )

            total_qso_count += 1

            # 实时进度。
            if total_qso_count % 200 == 0:
                sys.stdout.write(
                    f"\r    └── 进度: 当前文件已读 "
                    f"{rec_count} 条 | 总计处理 "
                    f"{total_qso_count} 条"
                )
                sys.stdout.flush()

        # 单个文件处理完毕。
        sys.stdout.write(
            f"\r    └── 完成: 当前文件已读 "
            f"{rec_count} 条 | 总计处理 "
            f"{total_qso_count} 条     \n"
        )
        sys.stdout.flush()

        # 强制垃圾回收。
        gc.collect()

    print("-" * 50)
    print("所有文件读取完毕，正在导出合并结果...")

    # 4. 导出文件。
    exported_files = 0

    for file_key, recs in deduplicator.final_records.items():
        if not recs:
            continue

        output_file = os.path.join(
            output_dir,
            f"{file_key}.adi"
        )

        write_adif_file(
            output_file,
            recs
        )

        print(
            f" -> 生成: {file_key}.adi "
            f"({len(recs)} 条 QSO)"
        )

        exported_files += 1

    # 5. 生成重复报告。
    if deduplicator.dupe_details:
        print(
            f"\n正在生成重复项报告 "
            f"({len(deduplicator.dupe_details)} 条重复)..."
        )

        report_path = os.path.join(
            output_dir,
            "dupe_report.html"
        )

        generate_html_report(
            report_path,
            deduplicator.dupe_details
        )
    else:
        print("\n太棒了！未发现重复记录。")

    # 6. 归档当前目录中的原始文件。
    timestamp = datetime.now().strftime(
        "%Y%m%d_%H%M%S"
    )

    print("\n正在归档原始文件...")

    for filename in root_files:
        src = os.path.join(
            current_dir,
            filename
        )

        dest_name = (
            f"{timestamp}-{filename}"
        )

        dest = os.path.join(
            done_dir,
            dest_name
        )

        try:
            shutil.move(
                src,
                dest
            )
        except Exception as e:
            print(
                f"归档失败 {filename}: {e}"
            )

    # 7. 汇总。
    if unknown_sources:
        print("\n" + "!" * 30)
        print(
            "警告: 以下文件包含缺失 "
            "STATION_CALLSIGN 的记录"
        )

        for source_file, count in unknown_sources.items():
            print(
                f" - {source_file}: {count} 条"
            )

        print("!" * 30)

    print("\n=== 处理完成 ===")
    print(
        f"处理文件数: {len(all_target_files)}"
    )
    print(
        f"总读取记录: {total_qso_count}"
    )
    print(
        f"发现重复项: "
        f"{len(deduplicator.dupe_details)}"
    )
    print(
        f"输出文件数: {exported_files}"
    )
    print(
        f"结果目录: {output_dir}"
    )

    input("\n按回车键退出程序...")


if __name__ == "__main__":
    process_adi_files()