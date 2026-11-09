import streamlit as st
from docx import Document
from docx.document import Document as _Document
from docx.table import _Cell, Table
from docx.text.paragraph import Paragraph
from docx.oxml.ns import qn
import pandas as pd
import re
import io
import datetime
from PIL import Image
import numpy as np
from collections import Counter
from openpyxl.styles import Alignment

st.set_page_config(page_title="调账核查系统", page_icon="🔍", layout="wide")
st.title("🔍 样机调账多维核查系统 (智能汇总版)")

# ================= 基础辅助函数 =================
def clean_text(text):
    return re.sub(r'\s+', '', str(text)).upper()

def extract_mls(text):
    matches = re.findall(r'M[L1I]\d{7}', text)
    return [re.sub(r'^M1', 'ML', m) for m in matches]

def extract_barcodes(text):
    if not text: return []
    parts = re.split(r'(?=(?:JC|HC[A-Z]))', text)
    bcs = []
    for p in parts:
        match = re.match(r'((?:JC|HC[A-Z])[A-Z0-9]{10,})', p)
        if match:
            bcs.append(match.group(1))
    return bcs

def extract_number_from_text(text, default=1):
    text = clean_text(text)
    match = re.search(r'(?:甲等品|乙等品|丙等品)(\d+)', text)
    if match: return int(match.group(1))
    return default

def extract_excel_qty(row, df_columns, is_loss):
    # 根据模糊列名去取数量
    target_keywords = ["盘亏", "数量"] if is_loss else ["盘盈", "数量"]
    for kw in target_keywords:
        for col in df_columns:
            if kw in str(col):
                val = row.get(col)
                if pd.notna(val) and str(val).strip() != "":
                    try:
                        v = float(val)
                        if v > 0: return int(v)
                    except: pass
    return 1

def iter_block_items(parent):
    if isinstance(parent, _Document): parent_elm = parent.element.body
    elif isinstance(parent, _Cell): parent_elm = parent._tc
    else: parent_elm = parent._element
    for child in parent_elm.iterchildren():
        if child.tag == qn('w:p'): yield Paragraph(child, parent)
        elif child.tag == qn('w:tbl'): yield Table(child, parent)

def build_dict(data):
    d = Counter()
    for k, v in data: d[k] += v
    return d

# 【核心修复】处理条码字符串：每24位换行，多个条码之间也换行
def format_barcode_cell(bc_list):
    valid_bcs = [b for b in bc_list if b]
    if not valid_bcs: return ""
    
    formatted_bcs = []
    for bc in valid_bcs:
        chunks = [bc[i:i+24] for i in range(0, len(bc), 24)]
        formatted_bcs.append("\n".join(chunks))
        
    return "\n".join(formatted_bcs)

# ================= OCR 模块 =================
@st.cache_resource(show_spinner="正在加载 OCR 引擎...")
def get_ocr():
    from rapidocr_onnxruntime import RapidOCR
    return RapidOCR()

# ================= 文件上传区 =================
col1, col2, col3 = st.columns(3)
with col1:
    docx_file = st.file_uploader("1. 上传 Word 调账单 (.docx)", type=["docx"])
with col2:
    excel_file = st.file_uploader("2. 上传 Excel 对比表 (.xlsx)", type=["xlsx", "xls"])
with col3:
    img_file = st.file_uploader("3. 上传 物流订单截图 (仅核对盘亏)", type=["png", "jpg", "jpeg"])

if docx_file or excel_file:
    # ---------------- 1. 解析 Word ----------------
    w_p3_old, w_p3_new = [], []
    w_p3_bcs_all = [] 
    w_p4_in, w_p4_out = [], []
    w_p5_loss, w_p5_gain = [], []
    current_context = ""
    
    if docx_file:
        doc = Document(docx_file)
        seen_tables = set()
        
        for block in iter_block_items(doc):
            if isinstance(block, Paragraph):
                text = clean_text(block.text)
                if "样机取回" in text or "空入" in text: current_context = "取回"
                elif "样机发出" in text or "空出" in text: current_context = "发出"
                elif "条码批扫" in text: current_context = "批扫"
            elif isinstance(block, Table):
                if block._tbl in seen_tables: continue
                seen_tables.add(block._tbl)

                header_text = ""
                for r in block.rows[:2]:
                    for c in r.cells: header_text += c.text
                header_clean = clean_text(header_text)

                for row in block.rows:
                    unique_cells = []
                    for cell in row.cells:
                        t = clean_text(cell.text)
                        if not unique_cells or t != unique_cells[-1]:
                            unique_cells.append(t)
                    
                    row_text = "".join(unique_cells)
                    if not row_text: continue
                    
                    mls = list(dict.fromkeys(extract_mls(row_text)))
                    if not mls: continue
                    
                    qty = extract_number_from_text(row_text)

                    if "盘亏" in header_clean and "盘盈" in header_clean:
                        if len(mls) >= 2:
                            w_p5_loss.append((mls[0], qty)); w_p5_gain.append((mls[1], qty))
                    elif "老国标" in header_clean and "新国标" in header_clean:
                        if len(mls) >= 2:
                            w_p3_old.append((mls[0], qty)); w_p3_new.append((mls[1], qty))
                            bcs = extract_barcodes(row_text)
                            w_p3_bcs_all.extend(bcs)
                    elif len(mls) == 1:
                        if current_context == "取回": w_p4_in.append((mls[0], qty))
                        elif current_context == "发出": w_p4_out.append((mls[0], qty))
                        else:
                            if "盘亏" in row_text: w_p4_in.append((mls[0], qty))
                            elif "盘盈" in row_text: w_p4_out.append((mls[0], qty))

    w_bc_dict = {}
    for ml, qty in w_p3_new:
        ml_num = re.sub(r'[^\d]', '', ml)
        valid_bcs = {bc for bc in w_p3_bcs_all if len(bc) >= 10 and bc[3:10] == ml_num}
        if ml not in w_bc_dict: w_bc_dict[ml] = set()
        w_bc_dict[ml].update(valid_bcs)

    # ---------------- 2. 解析 Excel ----------------
    ex_loss, ex_gain, ex_barcode = [], [], []
    ex_bc_dict = {}
    df_raw = None
    parsed_ex_data = {}
    
    if excel_file:
        xls = pd.ExcelFile(io.BytesIO(excel_file.getvalue()))
        df_raw = pd.read_excel(xls, sheet_name=0).fillna("")
        
        # 【核心修复】去除列名可能带的不可见字符，并模糊锁定核心列名
        df_raw.columns = [re.sub(r'\s+', '', str(c)).replace('　', '') for c in df_raw.columns]
        
        c_code = next((c for c in df_raw.columns if "物料" in c and "代码" in c), "物料代码")
        c_panyi = next((c for c in df_raw.columns if "盘盈" in c), "盘盈数量")
        c_pankui = next((c for c in df_raw.columns if "盘亏" in c), "盘亏数量")
        c_adj = next((c for c in df_raw.columns if "调整明细" in c), "调整明细")
        c_bc = next((c for c in df_raw.columns if "机号" in c or "条码" in c or "序列" in c), "机号码")
        c_op = next((c for c in df_raw.columns if "操作" in c), "操作")

        for idx, row in df_raw.iterrows():
            code = ""
            if c_code in df_raw.columns: code = clean_text(row.get(c_code, ""))
            else:
                # 兜底，找行里包含 ML 的
                for v in row.values:
                    if "ML" in str(v).upper():
                        code = clean_text(v)
                        break
            
            if not code or "ML" not in code: continue
            
            is_loss, is_gain = False, False
            if c_pankui in df_raw.columns and str(row.get(c_pankui, "")) not in ["", "0", "0.0"]: is_loss = True
            if c_panyi in df_raw.columns and str(row.get(c_panyi, "")) not in ["", "0", "0.0"]: is_gain = True
            
            if c_adj in df_raw.columns:
                adj_str = str(row.get(c_adj, ""))
                if adj_str.startswith("-"): is_loss = True
                elif adj_str not in ["", "0", "0.0"]: is_gain = True
                
            if c_op in df_raw.columns:
                if "盘亏" in str(row.get(c_op, "")): is_loss = True
                if "盘盈" in str(row.get(c_op, "")): is_gain = True

            bc = ""
            if c_bc in df_raw.columns:
                bc_val = clean_text(row.get(c_bc, ""))
                if bc_val: bc = bc_val

            qty = extract_excel_qty(row, df_raw.columns, is_loss)
            parsed_ex_data[idx] = {"is_loss": is_loss, "is_gain": is_gain, "barcode": bc, "qty": qty}
            
            if is_loss: ex_loss.append((code, qty))
            elif is_gain: 
                ex_gain.append((code, qty))
                if bc:
                    ex_barcode.append((code, bc))
                    if code not in ex_bc_dict: ex_bc_dict[code] = set()
                    ex_bc_dict[code].add(bc)

        if not ex_barcode and len(xls.sheet_names) > 1:
            df2 = pd.read_excel(xls, sheet_name=1, header=None).fillna("")
            for _, row in df2.iterrows():
                row_vals = [clean_text(v) for v in row.values]
                for i, val in enumerate(row_vals):
                    if val.startswith("ML") and len(val) == 9:
                        code = val
                        bc = ""
                        for j in range(i+1, len(row_vals)):
                            if re.match(r'(?:JC|HC[A-Z])[A-Z0-9]{10,}', row_vals[j]):
                                bc = row_vals[j]
                                break
                        if code and bc:
                            ex_barcode.append((code, bc))
                            if code not in ex_bc_dict: ex_bc_dict[code] = set()
                            ex_bc_dict[code].add(bc)

    # ---------------- 3. 解析 图片 ----------------
    img_loss = []
    if img_file:
        ocr = get_ocr()
        img = Image.open(img_file)
        result, _ = ocr(np.array(img.convert("RGB")))
        if result:
            words = []
            for item in result:
                box, text = item[0], item[1]
                cy = (box[0][1] + box[2][1]) / 2
                words.append({'cy': cy, 'text': text})
                
            words.sort(key=lambda x: x['cy'])
            lines = []
            for w in words:
                placed = False
                for l in lines:
                    if abs(l['cy'] - w['cy']) < 15:
                        l['text'] += w['text']
                        placed = True
                        break
                if not placed:
                    lines.append({'cy': w['cy'], 'text': w['text']})
            
            for l in lines:
                line_text = clean_text(l['text'])
                mls = extract_mls(line_text)
                if mls:
                    qty = extract_number_from_text(line_text)
                    for m in mls:
                        img_loss.append((m, qty))

    # ---------------- 汇总数据 ----------------
    d_w_p3_old, d_w_p4_in, d_w_p5_loss = build_dict(w_p3_old), build_dict(w_p4_in), build_dict(w_p5_loss)
    d_w_p3_new, d_w_p4_out, d_w_p5_gain = build_dict(w_p3_new), build_dict(w_p4_out), build_dict(w_p5_gain)
    d_ex_loss, d_ex_gain, d_img_loss = build_dict(ex_loss), build_dict(ex_gain), build_dict(img_loss)

    # ================= 开始比对 =================
    st.success("✅ 文件解析完成！以下是核查结果：")
    tab1, tab2, tab3 = st.tabs(["比对一：内部条码核查", "比对二：盘亏多方联查", "比对三：盘盈联查+条码聚合比对"])
    
    with tab1:
        st.subheader("Word 内部：新国标物料号 VS 机身码 (集合校验)")
        err_cnt = 0
        for ml, qty in d_w_p3_new.items():
            ml_num = re.sub(r'[^\d]', '', ml)
            w_bcs = w_bc_dict.get(ml, set())
            valid_bcs = {bc for bc in w_bcs if len(bc) >= 10 and bc[3:10] == ml_num}
            
            if len(valid_bcs) == qty:
                st.success(f"✅ 一致：物料 `{ml}` (总数:{qty}) ↔ 匹配到 {len(valid_bcs)} 个正确条码")
            else:
                err_cnt += 1
                st.error(f"❌ 数量与条码不匹配：物料 `{ml}` 总数为 {qty}，但只匹配到了 {len(valid_bcs)} 个对应条码。")
                
        if err_cnt == 0 and d_w_p3_new: st.balloons()

    with tab2:
        st.subheader("盘亏联查 (含图片数量核对)")
        all_loss_codes = set(d_w_p3_old) | set(d_w_p4_in) | set(d_w_p5_loss) | set(d_ex_loss) | set(d_img_loss)
        err_loss = False
        for code in all_loss_codes:
            c1, c2, c3 = d_w_p3_old.get(code,0), d_w_p4_in.get(code,0), d_w_p5_loss.get(code,0)
            c4, c5 = d_ex_loss.get(code,0), d_img_loss.get(code,0)
            counts = [c1, c2, c3, c4, c5]
            if not excel_file: counts[3] = -1
            if not img_file: counts[4] = -1
            valid_counts = [c for c in counts if c != -1]
            
            if len(set(valid_counts)) == 1: 
                st.success(f"✅ {code} 核对一致 (总数: {c1})")
            else:
                err_loss = True
                st.warning(f"⚠️ {code} 不一致：Word老({c1}), Word取({c2}), Word亏({c3}), Excel亏({c4}), 图片({c5})")
        if not err_loss and all_loss_codes: st.balloons()

    with tab3:
        st.subheader("盘盈联查 & 条码交叉核查")
        all_gain_codes = set(d_w_p3_new) | set(d_w_p4_out) | set(d_w_p5_gain) | set(d_ex_gain)
        err_gain = False
        for code in all_gain_codes:
            c1, c2, c3, c4 = d_w_p3_new.get(code,0), d_w_p4_out.get(code,0), d_w_p5_gain.get(code,0), d_ex_gain.get(code,0)
            counts = [c1, c2, c3, c4]
            if not excel_file: counts[3] = -1
            valid_counts = [c for c in counts if c != -1]
            
            if len(set(valid_counts)) == 1: st.success(f"✅ {code} 数量核对一致 (总数: {c1})")
            else:
                err_gain = True
                st.warning(f"⚠️ {code} 数量不一致：Word新({c1}), Word发({c2}), Word盈({c3}), Excel盈({c4})")
        if not err_gain and all_gain_codes: st.balloons()
            
        st.divider()
        st.subheader("条码唯一性核查 (防重复)")
        
        all_w_bcs = [bc for bc_set in w_bc_dict.values() for bc in bc_set if bc]
        w_bc_counts = Counter(all_w_bcs)
        ex_bc_counts = Counter([b for _, b in ex_barcode if b])
        
        dup_err = False
        for bc, count in w_bc_counts.items():
            if count > 1:
                dup_err = True
                st.error(f"❌ [Word 内部] 发现重复条码：`{bc}` (出现了 {count} 次)")
        for bc, count in ex_bc_counts.items():
            if count > 1:
                dup_err = True
                st.error(f"❌ [Excel 内部] 发现重复条码：`{bc}` (出现了 {count} 次)")
        if not dup_err:
            st.success("✅ 条码唯一性核查通过！(Word 和 Excel 各自内部均无重复)")

        st.subheader("Word VS Excel 条码集合核对")
        if excel_file:
            bc_err = 0
            all_mls = set(w_bc_dict.keys()) | set(ex_bc_dict.keys())
            for ml in all_mls:
                w_bcs = w_bc_dict.get(ml, set())
                ex_bcs = ex_bc_dict.get(ml, set())
                
                missing_in_ex = w_bcs - ex_bcs
                missing_in_w = ex_bcs - w_bcs
                
                if missing_in_ex:
                    bc_err += 1
                    st.error(f"❌ 物料 `{ml}`：在 Word 中有条码 `{', '.join(missing_in_ex)}`，但在 Excel 中缺失！")
                if missing_in_w:
                    bc_err += 1
                    st.error(f"❌ 物料 `{ml}`：在 Excel 中有条码 `{', '.join(missing_in_w)}`，但在 Word 中缺失！")
                    
            if bc_err == 0: 
                st.success("✅ 完美！Word 和 Excel 中的条码完全一一对应，无一遗漏/多余。")
                st.balloons()
        else:
            st.info("未上传 Excel 对比表，跳过条码交叉比对。")

    # ================= 生成并下载新表格 (汇总版) =================
    st.divider()
    st.subheader("📥 导出标准化数据表 (已按物料汇总)")
    
    loc_map = {
        "CD0I": "成都样机库", "GY1I": "贵阳样机库（临）", "KM1I": "昆明样机库", "XA0I": "西安样机库",
        "WS2I": "乌市样机库", "LZ0I": "兰州样机库", "SY1I": "沈阳样机库", "CC1I": "长春样机库(新）",
        "HRBI": "哈尔滨样机库", "NC1I": "南充样机库", "SJSI": "石家庄样机库（新）", "HS2I": "呼市样机库",
        "TY2I": "太原样机库", "WH0I": "武汉样机库", "ZZ1I": "郑州样机库", "CQ1I": "重庆样机库（临）",
        "NN1I": "南宁样机库（临）", "GZ1I": "广州样机库", "HZ0I": "杭州样机库", "TJ1I": "天津样机库临",
        "NC0I": "南昌样机库", "BJ1I": "北京样机库（新）", "NJ2I": "南京样机库", "CS2I": "长沙样机库",
        "WX0I": "无锡样机库", "JN1I": "济南样机库（新）", "FZ1I": "福州样机库（新）", "BB6I": "蚌埠样机库",
        "HF3I": "合肥样机库", "WH6I": "芜湖样机库", "MY0I": "绵阳样机库", "NJ4I": "南京样机库",
        "FZ2I": "福州样机库（新）", "GZ2I": "广州样机库", "CC2I": "长春样机库(新）", "NJ5I": "南京样机库",
        "HRRI": "哈尔滨样机库", "LZ2I": "兰州样机库", "WX2I": "无锡样机库", "CS3I": "长沙样机库"
    }
    today_str = datetime.date.today().strftime("%Y-%m-%d")

    if df_raw is not None:
        rows = []
        for idx, row in df_raw.iterrows():
            code = clean_text(row.get(c_code, ""))
            if not code or "ML" not in code: continue
            
            info = parsed_ex_data[idx]
            is_loss, is_gain = info["is_loss"], info["is_gain"]
            if not is_loss and not is_gain: continue
            
            qty = info["qty"]

            op = "盘亏" if is_loss else "盘盈"
            adj = str(-qty) if is_loss else str(qty)
            inv = qty if is_loss else 0
            
            loc_code = ""
            if "库位" in df_raw.columns:
                loc_code = str(row.get("库位", "")).strip().upper()
            loc_desc = loc_map.get(loc_code, "")
            
            date_val = row.get("日期", "")
            if pd.isna(date_val) or not str(date_val).strip():
                date_val = today_str
            else:
                date_val = str(date_val).split(" ")[0]

            sn = row.get("涉及单号", "")
            if pd.isna(sn): sn = ""
            else: sn = str(sn).split(".")[0]

            loc_desc_val = row.get("库位描述", "")
            if pd.isna(loc_desc_val) or not str(loc_desc_val).strip():
                loc_desc_val = loc_desc

            inv_sys = row.get("条码系统库存", "")
            inv_r3 = row.get("R3库存", "")
            if pd.isna(inv_sys) or str(inv_sys).strip() == "": inv_sys = inv
            if pd.isna(inv_r3) or str(inv_r3).strip() == "": inv_r3 = inv
            
            grade = row.get("等级", "")
            if pd.isna(grade) or not str(grade).strip(): grade = "甲等品"

            adj_val = row.get(c_adj, "")
            if pd.isna(adj_val) or str(adj_val).strip() == "": adj_val = adj

            status = row.get("处理情况", "")
            if pd.isna(status): status = ""

            op_val = row.get(c_op, "")
            if pd.isna(op_val) or not str(op_val).strip(): op_val = op

            bc = info.get("barcode", "")
            if not bc:
                bc = row.get(c_bc, "")
                if pd.isna(bc): bc = ""
                bc = str(bc).strip()
                if not bc:
                    for c, b in ex_barcode:
                        if c == code:
                            bc = b
                            break

            rows.append({
                "日期": date_val,
                "库位": loc_code,
                "库位描述": loc_desc_val,
                "涉及单号": sn,
                "物料代码": code,
                "条码系统库存": inv_sys,
                "R3库存": inv_r3,
                "等级": grade,
                "调整明细（条码系统）": adj_val,
                "处理情况": status,
                "操作": op_val,
                "条码": bc
            })
        
        out_df = pd.DataFrame(rows)
        if not out_df.empty:
            group_cols = ["日期", "库位", "库位描述", "涉及单号", "物料代码", "等级", "操作"]
            
            for c in ["条码系统库存", "R3库存", "调整明细（条码系统）"]:
                out_df[c] = pd.to_numeric(out_df[c], errors='coerce').fillna(0).astype(int)
                
            out_df["条码"] = out_df["条码"].astype(str).replace('nan', '')

            out_df = out_df.groupby(group_cols, as_index=False).agg({
                "条码系统库存": "sum",
                "R3库存": "sum",
                "调整明细（条码系统）": "sum",
                "处理情况": "first",
                "条码": format_barcode_cell
            })

            out_df["sort"] = out_df["操作"].apply(lambda x: 0 if x == "盘亏" else 1)
            out_df = out_df.sort_values(by="sort").drop(columns=["sort"]).reset_index(drop=True)

            non_empty_barcodes = out_df[out_df["条码"] != ""]["条码"]
            if non_empty_barcodes.duplicated().any():
                st.error("⚠️ 警告：合并后的表格中仍存在重复的条码组合，请检查上方日志！")

            out_buf = io.BytesIO()
            with pd.ExcelWriter(out_buf, engine='openpyxl') as writer:
                out_df.to_excel(writer, index=False, sheet_name='核查结果')
                
                worksheet = writer.sheets['核查结果']
                for row in worksheet.iter_rows():
                    for cell in row:
                        if cell.column_letter == 'L' or cell.value == "条码": 
                            cell.alignment = Alignment(wrap_text=True, vertical='top')
                        else:
                            cell.alignment = Alignment(wrap_text=False, vertical='top')
            
            out_buf.seek(0)
            
            st.download_button(
                label="点击下载标准化汇总 Excel 表格",
                data=out_buf,
                file_name="调账明细汇总核查结果.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            st.dataframe(out_df.head(15))
        else:
            st.warning("未解析到有效的盘盈或盘亏数据，无法导出。")

    # ================= 调试数据面板 =================
    with st.expander("🛠 查看所有提取的原始数据"):
        c1, c2 = st.columns(2)
        with c1:
            st.write("**【Word】P3老国标:**", w_p3_old)
            st.write("**【Word】P4取回:**", w_p4_in)
            st.write("**【Word】P5盘亏:**", w_p5_loss)
        with c2:
            st.write("**【Word】P3新国标:**", w_p3_new)
            st.write("**【Word】P4发出:**", w_p4_out)
            st.write("**【Word】P5盘盈:**", w_p5_gain)
        st.write("**【Excel】盘亏:**", ex_loss)
        st.write("**【Excel】盘盈:**", ex_gain)
        st.write("**【Excel】条码:**", ex_barcode)
        st.write("**【图片】OCR盘亏(含数量):**", img_loss)

else:
    st.info("👈 请先上传 Word 调账单 或 Excel 对比表进行联查。")
