import os
import re
import shutil
from collections import defaultdict
from datetime import datetime, timedelta

def parse_adif_records(file_path):
    """
    Parses an ADIF file and returns a list of records.
    Each record is a dictionary of tags and values.
    """
    file_name = os.path.basename(file_path)
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return []

    # Split header and records (records start after <EOH> or <eoh>)
    parts = re.split(r'<EOH>|<eoh>', content, flags=re.IGNORECASE)
    records_part = parts[-1] if len(parts) > 1 else parts[0]

    # Split into individual records using <EOR> or <eor>
    raw_records = re.split(r'<EOR>|<eor>', records_part, flags=re.IGNORECASE)
    
    parsed_records = []
    # Regex to match ADIF tags: <TAG:LEN:TYPE>DATA or <TAG:LEN>DATA
    tag_pattern = re.compile(r'<([^:>]+):(\d+)(?::[^>]+)?>([^<]*)', re.IGNORECASE)

    for raw_rec in raw_records:
        if not raw_rec.strip():
            continue
        
        record_data = {}
        # Track which file this specific record came from
        record_data['_SOURCE_FILE'] = file_name
        
        matches = tag_pattern.findall(raw_rec)
        for tag, length, value in matches:
            clean_value = value[:int(length)]
            record_data[tag.upper()] = clean_value
        
        if len(record_data) > 1: # More than just the _SOURCE_FILE
            parsed_records.append(record_data)
            
    return parsed_records

def write_adif_file(file_path, records):
    """
    Writes a list of records to an ADIF file.
    """
    header = "ADIF Export from Python Tool\r\nCreated by Gemini\r\n<ADIF_VER:5>3.1.4\r\n<EOH>\r\n"
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(header)
        for rec in records:
            line = ""
            for tag, val in rec.items():
                if tag.startswith('_'): continue # Skip internal tracking tags
                line += f"<{tag}:{len(str(val))}>{val} "
            f.write(line + "<EOR>\r\n")

def get_qso_time(rec):
    """
    Extracts datetime object from record for comparison.
    Combines QSO_DATE and TIME_ON.
    """
    date_str = rec.get('QSO_DATE', '') # YYYYMMDD
    time_str = rec.get('TIME_ON', '000000')[:6].ljust(6, '0') # HHMMSS
    
    try:
        return datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M%S")
    except:
        return None

def find_duplicate_info(new_rec, existing_recs):
    """
    Checks if a record is a duplicate and returns the existing record if found.
    """
    new_call = new_rec.get('CALL', '').upper()
    new_band = new_rec.get('BAND', '').upper()
    new_mode = new_rec.get('MODE', '').upper()
    new_time = get_qso_time(new_rec)
    
    if not new_time or not new_call:
        return None

    for rec in existing_recs:
        if (rec.get('CALL', '').upper() == new_call and 
            rec.get('BAND', '').upper() == new_band and 
            rec.get('MODE', '').upper() == new_mode):
            
            exist_time = get_qso_time(rec)
            if exist_time:
                diff = abs((new_time - exist_time).total_seconds())
                if diff <= 1800: # 30 minutes
                    return rec
    return None

def generate_html_report(report_path, dupe_details):
    """
    Generates a visual HTML report for duplicate analysis.
    """
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
        </style>
    </head>
    <body>
        <h1>ADIF 查重比对报告</h1>
        <div class="summary">
            <p><strong>生成时间:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p><strong>发现重复条目:</strong> {len(dupe_details)}</p>
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
    
    for i, d in enumerate(dupe_details, 1):
        new_rec = d['new_rec']
        old_rec = d['old_rec']
        
        def format_rec(r):
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
    """
    Main logic to scan, merge, split, and deduplicate ADI files.
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(current_dir, 'output')
    done_dir = os.path.join(current_dir, 'done')
    
    # 1. Prepare directories
    if os.path.exists(output_dir):
        print(f"Cleaning existing output directory: {output_dir}")
        shutil.rmtree(output_dir)
    os.makedirs(output_dir)
    
    if not os.path.exists(done_dir):
        os.makedirs(done_dir)
        print(f"Created done directory: {done_dir}")

    grouped_records = defaultdict(list)
    unknown_sources = defaultdict(int)
    dupe_details = [] 
    
    # 2. Identify all .adi files in root and 'done' folder
    root_files = [f for f in os.listdir(current_dir) if f.lower().endswith('.adi')]
    done_files = [os.path.join('done', f) for f in os.listdir(done_dir) if f.lower().endswith('.adi')]
    all_target_files = root_files + done_files
    
    if not all_target_files:
        print("No .adi files found in current or 'done' directory.")
        return

    print(f"Found {len(all_target_files)} files ({len(root_files)} new, {len(done_files)} in done). Starting analysis...")

    # 3. Process each file
    total_qso_count = 0
    for file_path_rel in all_target_files:
        full_path = os.path.join(current_dir, file_path_rel)
        records = parse_adif_records(full_path)
        
        for rec in records:
            raw_callsign = rec.get('STATION_CALLSIGN')
            if not raw_callsign or not raw_callsign.strip():
                call_key = 'UNKNOWN'
                unknown_sources[file_path_rel] += 1
            else:
                call_key = raw_callsign.strip().replace('/', '_')
            
            # Check for duplicates
            duplicate_rec = find_duplicate_info(rec, grouped_records[call_key])
            if duplicate_rec:
                dupe_details.append({
                    'station': call_key,
                    'new_rec': rec,
                    'old_rec': duplicate_rec
                })
                continue
                
            grouped_records[call_key].append(rec)
            total_qso_count += 1
            
        print(f"Processed {file_path_rel}: {len(records)} records handled.")

    # 4. Output to files
    print("\nWriting merged results to output folder...")
    for callsign, recs in grouped_records.items():
        output_file = os.path.join(output_dir, f"{callsign}.adi")
        write_adif_file(output_file, recs)
        print(f"Generated: {callsign}.adi ({len(recs)} unique QSOs)")

    # 5. Generate HTML Duplicate Report
    report_path = os.path.join(output_dir, "dupe_report.html")
    generate_html_report(report_path, dupe_details)

    # 6. Archive root files to 'done' folder with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    print("\nArchiving root files to 'done' folder...")
    for f in root_files:
        src = os.path.join(current_dir, f)
        dest_name = f"{timestamp}-{f}"
        dest = os.path.join(done_dir, dest_name)
        try:
            shutil.move(src, dest)
            print(f"Moved: {f} -> done/{dest_name}")
        except Exception as e:
            print(f"Failed to move {f}: {e}")

    # Summary Display
    if unknown_sources:
        print("\n" + "!"*30)
        print("WARNING: Found records with UNKNOWN STATION_CALLSIGN!")
        for source_file, count in unknown_sources.items():
            print(f" - {source_file}: {count} unknown record(s)")
        print("!"*30)

    print(f"\nSuccess!")
    print(f"Total Unique QSOs: {total_qso_count}")
    print(f"Total Duplicates Removed: {len(dupe_details)}")
    print(f"Detailed HTML report saved as: output/dupe_report.html")
    print(f"Results saved in: {output_dir}")

if __name__ == "__main__":
    process_adi_files()