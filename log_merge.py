import os
import re
import shutil
import sys
from collections import defaultdict
from datetime import datetime

# 增加 CSV 模块用于可能的扩展，但在本逻辑中主要依赖 ADIF 解析
# 增加 gc 用于手动垃圾回收，应对超大内存压力
import gc

def get_base_path():
    """
    获取脚本或 EXE 的实际运行路径。
    """
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    else:
        return os.path.dirname(os.path.abspath(__file__))

def get_qso_time(rec):
    """
    从记录中提取 datetime 对象。
    合并 QSO_DATE 和 TIME_ON。
    """
    date_str = rec.get('QSO_DATE', '')  # YYYYMMDD
    # TIME_ON 可能是 4位或6位，补全为6位
    time_str = rec.get('TIME_ON', '000000')
    if len(time_str) < 6:
        time_str = time_str.ljust(6, '0')
    else:
        time_str = time_str[:6]
    
    try:
        return datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M%S")
    except:
        return None

class AdifParser:
    """
    流式 ADIF 解析器，避免一次性读取大文件导致内存溢出。
    """
    def __init__(self, file_path):
        self.file_path = file_path
        self.file_name = os.path.basename(file_path)
        self.tag_pattern = re.compile(r'<([^:>]+):(\d+)(?::[^>]+)?>', re.IGNORECASE)

    def _parse_single_record(self, raw_data):
        """解析单条 ADIF 文本为字典"""
        if not raw_data.strip():
            return None
            
        record_data = {}
        record_data['_SOURCE_FILE'] = self.file_name
        
        # 指针位置
        pos = 0
        data_len = len(raw_data)
        
        while pos < data_len:
            # 查找下一个标签的起始
            tag_match = self.tag_pattern.search(raw_data, pos)
            if not tag_match:
                break
                
            tag_name = tag_match.group(1).upper()
            value_len = int(tag_match.group(2))
            
            # 数据值的起始位置
            value_start = tag_match.end()
            value_end = value_start + value_len
            
            # 提取值
            if value_end <= data_len:
                value = raw_data[value_start:value_end]
                record_data[tag_name] = value
                pos = value_end
            else:
                # 异常情况：长度超出剩余字符串
                break
                
        if len(record_data) > 1:
            return record_data
        return None

    def stream_records(self):
        """
        生成器：逐条读取并返回记录。
        使用缓冲区处理跨块的记录。
        """
        buffer = ""
        chunk_size = 1024 * 1024 * 2  # 2MB chunks
        
        try:
            with open(self.file_path, 'r', encoding='utf-8', errors='ignore') as f:
                # 尝试跳过 Header，找到第一个 <EOH>
                header_found = False
                
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    
                    buffer += chunk
                    
                    if not header_found:
                        # 查找 header 结束符 (忽略大小写)
                        # 使用正则不仅慢，而且处理分块麻烦，这里简单处理
                        lower_buf = buffer.lower()
                        eoh_idx = lower_buf.find('<eoh>')
                        if eoh_idx != -1:
                            buffer = buffer[eoh_idx+5:] # 跳过 <EOH>
                            header_found = True
                        else:
                            # 如果 buffer 太大还没找到 header，可能是纯记录文件或异常，保留 buffer 继续读
                            if len(buffer) > 10 * 1024 * 1024: # 10MB header? 不太可能
                                header_found = True # 强制开始解析
                            continue

                    # 处理记录分隔符 <EOR>
                    while True:
                        # 查找 <EOR> 的位置
                        # 注意：buffer 可能包含半个 <EOR>，例如 "...<EO"
                        lower_buf = buffer.lower()
                        eor_idx = lower_buf.find('<eor>')
                        
                        if eor_idx == -1:
                            break
                        
                        # 提取一条完整的原始记录记录
                        raw_rec = buffer[:eor_idx]
                        # 移动 buffer 指针
                        buffer = buffer[eor_idx+5:]
                        
                        # 解析该记录
                        parsed_rec = self._parse_single_record(raw_rec)
                        if parsed_rec:
                            yield parsed_rec
                            
                # 处理文件末尾可能剩余的内容
                if buffer.strip():
                     parsed_rec = self._parse_single_record(buffer)
                     if parsed_rec:
                         yield parsed_rec
                         
        except Exception as e:
            print(f"读取文件 {self.file_name} 时出错: {e}")

class FastDeduplicator:
    """
    高效去重管理器。
    使用哈希索引代替线性遍历，大幅提升大数据量下的速度。
    """
    def __init__(self):
        # 最终输出的记录列表： Key=Station_Callsign, Value=[Records]
        self.final_records = defaultdict(list)
        
        # 索引用于快速查找： Key=Station_Callsign -> { (DX_Call, Band, Mode) -> [(TimeObj, Record)] }
        self.lookup_index = defaultdict(lambda: defaultdict(list))
        
        self.dupe_details = []

    def process_record(self, record, station_call):
        """
        处理单条记录：判断重复，如果非重复则添加。
        """
        # 1. 提取关键比对字段
        dx_call = record.get('CALL', '').upper()
        band = record.get('BAND', '').upper()
        mode = record.get('MODE', '').upper()
        qso_time = get_qso_time(record)
        
        if not dx_call or not qso_time:
            # 缺少关键信息的记录，直接视为新记录添加，不参与严格去重
            self._add_to_storage(station_call, record, dx_call, band, mode, qso_time)
            return

        # 2. 快速查重
        # 只有相同 (DX_Call, Band, Mode) 的记录才值得比对时间
        key = (dx_call, band, mode)
        candidates = self.lookup_index[station_call][key]
        
        is_dupe = False
        existing_rec = None
        
        for exist_time, exist_rec in candidates:
            # 时间差在 15 分钟 (900秒) 内视为重复
            diff = abs((qso_time - exist_time).total_seconds())
            if diff <= 900:
                is_dupe = True
                existing_rec = exist_rec
                break
        
        if is_dupe:
            self.dupe_details.append({
                'station': station_call,
                'new_rec': record,
                'old_rec': existing_rec
            })
        else:
            self._add_to_storage(station_call, record, dx_call, band, mode, qso_time)

    def _add_to_storage(self, station_call, record, dx_call, band, mode, qso_time):
        """将记录加入存储和索引"""
        self.final_records[station_call].append(record)
        
        # 只有具备完整信息的才加入索引供后续比对
        if dx_call and qso_time:
            key = (dx_call, band, mode)
            # 存储 (Time, Record) 元组
            self.lookup_index[station_call][key].append((qso_time, record))

def write_adif_file(file_path, records):
    """写入 ADIF 文件"""
    header = (
        "ADIF Export from Python Tool (Optimized)\r\n"
        "Created by Gemini\r\n"
        "<ADIF_VER:5>3.1.4\r\n"
        "<EOH>\r\n"
    )
    
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(header)
            for rec in records:
                line = ""
                for tag, val in rec.items():
                    if tag.startswith('_'): continue 
                    # 确保 val 是字符串
                    val_str = str(val)
                    line += f"<{tag}:{len(val_str.encode('utf-8'))}>{val_str} "
                f.write(line + "<EOR>\r\n")
    except Exception as e:
        print(f"写入文件 {file_path} 失败: {e}")

def generate_html_report(report_path, dupe_details):
    """生成 HTML 报告 (保持原逻辑，优化大数据下的展示可能需要分页，但此处暂保持简单)"""
    if not dupe_details:
        return

    # 如果重复项太多，截断报告以防止 HTML 过大卡死浏览器
    MAX_REPORT_ITEMS = 5000
    display_items = dupe_details[:MAX_REPORT_ITEMS]
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>ADIF Deduplication Visual Report</title>
        <style>
            body {{ font-family: sans-serif; background: #f4f7f6; padding: 20px; }}
            h1 {{ color: #2c3e50; }}
            .summary {{ background: white; padding: 15px; border-radius: 8px; margin-bottom: 20px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }}
            table {{ width: 100%; border-collapse: collapse; background: white; box-shadow: 0 2px 15px rgba(0,0,0,0.1); }}
            th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
            th {{ background-color: #34495e; color: white; }}
            tr:hover {{ background-color: #f1f1f1; }}
            .diff-container {{ display: flex; gap: 10px; font-size: 0.85em; }}
            .rec-box {{ background: #ebf5fb; padding: 10px; border-radius: 4px; border: 1px solid #aed6f1; flex: 1; }}
            .existing {{ background: #fef9e7; border-color: #f9e79f; }}
            .tag {{ font-weight: bold; color: #7f8c8d; }}
            .val {{ color: #2980b9; }}
            .file-info {{ display: block; margin-bottom: 8px; font-weight: bold; color: #2c3e50; border-bottom: 1px dashed #ccc; padding-bottom: 4px; }}
            .warning {{ color: red; font-weight: bold; margin-top: 10px; }}
        </style>
    </head>
    <body>
        <h1>ADIF 查重比对报告</h1>
        <div class="summary">
            <p><strong>生成时间:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p><strong>重复条目总数:</strong> {len(dupe_details)}</p>
            {f'<p class="warning">注意：报告仅显示前 {MAX_REPORT_ITEMS} 条，以免文件过大。</p>' if len(dupe_details) > MAX_REPORT_ITEMS else ''}
        </div>
        <table>
            <thead>
                <tr>
                    <th>序号</th>
                    <th>所属电台呼号</th>
                    <th>比对详情 (重复项 vs 原始项)</th>
                </tr>
            </thead>
            <tbody>
    """
    
    for i, d in enumerate(display_items, 1):
        new_rec = d['new_rec']
        old_rec = d['old_rec']
        
        def format_rec(r):
            if not r: return "无法读取记录"
            return f"<span class='tag'>CALL:</span> <span class='val'>{r.get('CALL','')}</span> | " \
                   f"<span class='tag'>BAND:</span> <span class='val'>{r.get('BAND','')}</span> | " \
                   f"<span class='tag'>MODE:</span> <span class='val'>{r.get('MODE','')}</span><br>" \
                   f"<span class='tag'>TIME:</span> <span class='val'>{r.get('QSO_DATE','')} {r.get('TIME_ON','')}</span>"

        html_content += f"""
                <tr>
                    <td>{i}</td>
                    <td><strong>{d['station']}</strong></td>
                    <td>
                        <div class="diff-container">
                            <div class="rec-box">
                                <span class="file-info">重复项来源: {new_rec.get('_SOURCE_FILE', '未知')}</span>
                                {format_rec(new_rec)}
                            </div>
                            <div class="rec-box existing">
                                <span class="file-info">原始项来源: {old_rec.get('_SOURCE_FILE', '未知')}</span>
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
    
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(html_content)

def process_adi_files():
    current_dir = get_base_path()
    output_dir = os.path.join(current_dir, 'output')
    done_dir = os.path.join(current_dir, 'done')
    
    # 1. 准备目录
    if os.path.exists(output_dir):
        print(f"正在清理输出目录: {output_dir}")
        shutil.rmtree(output_dir)
    os.makedirs(output_dir)
    
    if not os.path.exists(done_dir):
        os.makedirs(done_dir)

    # 初始化去重器
    deduplicator = FastDeduplicator()
    unknown_sources = defaultdict(int)
    
    # 2. 扫描文件
    # 支持 .adi, .adif 以及大写变体
    valid_exts = ('.adi', '.adif')
    root_files = [f for f in os.listdir(current_dir) if f.lower().endswith(valid_exts)]
    done_files = [os.path.join('done', f) for f in os.listdir(done_dir) if f.lower().endswith(valid_exts)]
    all_target_files = root_files + done_files
    
    if not all_target_files:
        print(f"错误: 在 {current_dir} 及其 done 目录中未找到 ADIF 文件 (.adi/.adif)。")
        input("按回车键退出...")
        return

    print(f"找到 {len(all_target_files)} 个文件。开始分析...")
    print("-" * 50)

    # 3. 处理文件
    total_qso_count = 0
    file_count = 0
    
    for file_path_rel in all_target_files:
        file_count += 1
        full_path = os.path.join(current_dir, file_path_rel)
        print(f"[{file_count}/{len(all_target_files)}] 正在读取: {file_path_rel}")
        
        parser = AdifParser(full_path)
        rec_count = 0
        
        # 使用流式读取
        for rec in parser.stream_records():
            rec_count += 1
            
            raw_callsign = rec.get('STATION_CALLSIGN')
            if not raw_callsign or not raw_callsign.strip():
                call_key = 'UNKNOWN'
                unknown_sources[file_path_rel] += 1
            else:
                call_key = raw_callsign.strip().replace('/', '_')
            
            # 交给去重器处理
            deduplicator.process_record(rec, call_key)
            total_qso_count += 1
            
            # 实时进度条 (每处理 200 条刷新一次显示，使用 \r 回车符覆盖当前行)
            if total_qso_count % 200 == 0:
                sys.stdout.write(f"\r    └── 进度: 当前文件已读 {rec_count} 条 | 总计处理 {total_qso_count} 条")
                sys.stdout.flush()

        # 单个文件处理完毕，显示最终行并换行
        sys.stdout.write(f"\r    └── 完成: 当前文件已读 {rec_count} 条 | 总计处理 {total_qso_count} 条     \n")
        sys.stdout.flush()
        
        # 强制垃圾回收，释放读取大文件产生的临时内存
        gc.collect()

    print("-" * 50)
    print("所有文件读取完毕，正在导出合并结果...")

    # 4. 导出文件
    exported_files = 0
    for callsign, recs in deduplicator.final_records.items():
        if not recs: continue
        output_file = os.path.join(output_dir, f"{callsign}.adi")
        write_adif_file(output_file, recs)
        print(f" -> 生成: {callsign}.adi ({len(recs)} 条唯一 QSO)")
        exported_files += 1

    # 5. 生成报告
    if deduplicator.dupe_details:
        print(f"\n正在生成重复项报告 ({len(deduplicator.dupe_details)} 条重复)...")
        report_path = os.path.join(output_dir, "dupe_report.html")
        generate_html_report(report_path, deduplicator.dupe_details)
    else:
        print("\n太棒了！未发现重复记录。")

    # 6. 归档
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    print("\n正在归档原始文件...")
    for f in root_files:
        src = os.path.join(current_dir, f)
        dest_name = f"{timestamp}-{f}"
        dest = os.path.join(done_dir, dest_name)
        try:
            shutil.move(src, dest)
        except Exception as e:
            print(f"归档失败 {f}: {e}")

    # 总结
    if unknown_sources:
        print("\n" + "!"*30)
        print("警告: 以下文件包含缺失 STATION_CALLSIGN 的记录，已归类为 UNKNOWN.adi")
        for source_file, count in unknown_sources.items():
            print(f" - {source_file}: {count} 条")
        print("!"*30)

    print(f"\n=== 处理完成 ===")
    print(f"处理文件数: {len(all_target_files)}")
    print(f"总读取记录: {total_qso_count}")
    print(f"发现重复项: {len(deduplicator.dupe_details)}")
    print(f"输出文件数: {exported_files}")
    print(f"结果目录: {output_dir}")
    
    input("\n按回车键退出程序...")

if __name__ == "__main__":
    process_adi_files()