# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import io
from datetime import datetime

# ========== 页面配置 ==========
st.set_page_config(
    page_title="IB00工厂库存匹配",
    page_icon="📦",
    layout="wide"
)

# ========== 自定义CSS ==========
st.markdown("""
<style>
    * {
        font-family: "Microsoft YaHei", "SimHei", "PingFang SC", "Helvetica Neue", Roboto, sans-serif;
    }
    .stButton button {
        font-size: 16px;
        font-weight: 500;
    }
</style>
""", unsafe_allow_html=True)

# ========== 辅助函数 ==========
def clean_str(val):
    """清洗字符串，去除空格并处理浮点数后缀 .0"""
    if pd.isna(val):
        return ''
    s = str(val).strip()
    try:
        if '.' in s:
            f = float(s)
            if f.is_integer():
                s = str(int(f))
    except:
        pass
    return s

def clean_float(val):
    """安全转换为浮点数"""
    try:
        return float(val)
    except:
        return 0.0

def read_file(file):
    """通用文件读取函数，支持 csv/xls/xlsx"""
    if file is None:
        return None
    try:
        if file.name.endswith('.csv'):
            return pd.read_csv(file, encoding='utf-8-sig')
        elif file.name.endswith('.xls'):
            return pd.read_excel(file, engine='xlrd')
        else:
            return pd.read_excel(file, engine='openpyxl')
    except Exception:
        try:
            return pd.read_excel(file)
        except:
            file.seek(0)
            return pd.read_csv(file, encoding='utf-8-sig')

# ========== 主应用 ==========
def main():
    st.title("📦 IB00工厂库存匹配")
    st.markdown("上传 **IB00库存表** 和 **库位表**，系统将自动匹配并生成盘存汇总表")
    
    # 文件上传区域
    col1, col2 = st.columns(2)
    
    with col1:
        ib00_file = st.file_uploader("上传 IB00库存表", type=['xlsx', 'xls', 'csv'], key="ib00")
        st.caption("需要包含：存储位置、非限制使用的库存、冻结库存等列")
    
    with col2:
        location_file = st.file_uploader("上传 库位表", type=['xlsx', 'xls', 'csv'], key="location")
        st.caption("需要包含：实物库位表和赠品库位表两个工作表")
    
    # 配置选项
    with st.expander("⚙️ 列名配置", expanded=False):
        st.info("请根据您的实际Excel列名修改以下配置")
        
        col_storage = st.text_input("存储位置列名", value="存储位置")
        col_unrestricted = st.text_input("非限制使用的库存列名", value="非限制使用的库存")
        col_frozen = st.text_input("冻结库存列名", value="冻结库存")
        col_loc_code = st.text_input("库位代码列名", value="库位代码")
        col_loc_desc = st.text_input("仓库描述列名", value="仓库描述")
        
        sheet_physical = st.text_input("实物库位表工作表名", value="实物库位表")
        sheet_gift = st.text_input("赠品库位表工作表名", value="赠品库位表")
    
    if st.button("🚀 开始匹配", type="primary", use_container_width=True):
        if ib00_file is None or location_file is None:
            st.error("❌ 请同时上传 IB00库存表 和 库位表")
        else:
            with st.spinner("⏳ 正在处理数据，请稍候..."):
                try:
                    # 读取文件
                    df_ib00 = read_file(ib00_file)
                    
                    # 读取库位表的两个工作表
                    df_physical = pd.read_excel(location_file, sheet_name=sheet_physical)
                    try:
                        df_gift = pd.read_excel(location_file, sheet_name=sheet_gift)
                    except:
                        df_gift = None
                        st.warning(f"⚠️ 未找到工作表 '{sheet_gift}'，将跳过赠品匹配")
                    
                    # 清洗数据
                    df_ib00[col_storage] = df_ib00[col_storage].astype(str).apply(clean_str)
                    df_ib00[col_unrestricted] = df_ib00[col_unrestricted].apply(clean_float)
                    df_ib00[col_frozen] = df_ib00[col_frozen].apply(clean_float)
                    
                    df_physical[col_loc_code] = df_physical[col_loc_code].astype(str).apply(clean_str)
                    if col_loc_desc in df_physical.columns:
                        df_physical[col_loc_desc] = df_physical[col_loc_desc].astype(str).apply(clean_str)
                    
                    if df_gift is not None:
                        df_gift[col_loc_code] = df_gift[col_loc_code].astype(str).apply(clean_str)
                        if col_loc_desc in df_gift.columns:
                            df_gift[col_loc_desc] = df_gift[col_loc_desc].astype(str).apply(clean_str)
                    
                    # 计算总库存
                    df_ib00['总库存'] = df_ib00[col_unrestricted] + df_ib00[col_frozen]
                    
                    # 创建匹配字典
                    loc_dict = {}
                    for _, row in df_physical.iterrows():
                        code = row[col_loc_code]
                        desc = row[col_loc_desc] if col_loc_desc in df_physical.columns else code
                        loc_dict[code] = desc
                    
                    gift_dict = {}
                    if df_gift is not None:
                        for _, row in df_gift.iterrows():
                            code = row[col_loc_code]
                            desc = row[col_loc_desc] if col_loc_desc in df_gift.columns else code
                            gift_dict[code] = desc
                    
                    # 成品匹配
                    df_ib00['仓库描述'] = ''
                    for idx, row in df_ib00.iterrows():
                        storage = row[col_storage]
                        if storage in loc_dict:
                            df_ib00.at[idx, '仓库描述'] = loc_dict[storage]
                    
                    # 成品汇总
                    product_summary = df_ib00[df_ib00['仓库描述'] != ''].groupby('仓库描述').agg(
                        总库存总和=('总库存', 'sum')
                    ).reset_index()
                    product_summary = product_summary.sort_values('仓库描述', ascending=True)
                    total_product = product_summary['总库存总和'].sum()
                    
                    # 赠品匹配（尾数6）
                    if df_gift is not None:
                        df_gift_stock = df_ib00[df_ib00[col_storage].str.endswith('6')].copy()
                        df_gift_stock['仓库描述'] = ''
                        for idx, row in df_gift_stock.iterrows():
                            storage = row[col_storage]
                            if storage in gift_dict:
                                df_gift_stock.at[idx, '仓库描述'] = gift_dict[storage]
                        
                        gift_summary = df_gift_stock[df_gift_stock['仓库描述'] != ''].groupby('仓库描述').agg(
                            总库存总和=('总库存', 'sum')
                        ).reset_index()
                        gift_summary = gift_summary.sort_values('仓库描述', ascending=True)
                        total_gift = gift_summary['总库存总和'].sum()
                    else:
                        gift_summary = pd.DataFrame()
                        total_gift = 0
                    
                    # 生成输出文件
                    output_buffer = io.BytesIO()
                    
                    with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
                        # 成品汇总表
                        product_data = product_summary[['仓库描述', '总库存总和']].copy()
                        product_data.columns = ['仓库描述', 'ERP账面数']
                        for col in ['入库未计数', '出库未记', '盘盈', '盘亏', '实盘', '备注']:
                            product_data[col] = ''
                        product_data.loc[len(product_data)] = ['合计', total_product, '', '', '', '', '', '']
                        product_data.to_excel(writer, sheet_name='成品汇总表', index=False, startrow=1)
                        
                        worksheet = writer.sheets['成品汇总表']
                        worksheet.cell(row=1, column=1, value='IB00工厂汇总\t2/3/6/7/8/Z库未计算在ERP账面数内')
                        
                        # 赠品汇总表
                        if not gift_summary.empty:
                            gift_data = gift_summary[['仓库描述', '总库存总和']].copy()
                            gift_data.columns = ['仓库描述', 'ERP账面数']
                            for col in ['入库未计数', '出库未记', '盘盈', '盘亏', '实盘', '备注']:
                                gift_data[col] = ''
                            gift_data.loc[len(gift_data)] = ['合计', total_gift, '', '', '', '', '', '']
                            gift_data.to_excel(writer, sheet_name='赠品汇总表', index=False, startrow=1)
                            
                            worksheet = writer.sheets['赠品汇总表']
                            worksheet.cell(row=1, column=1, value='IB00工厂汇总\t2/3/6/7/8/Z库未计算在ERP账面数内')
                        else:
                            pd.DataFrame({'说明': ['赠品库位表不存在或无尾数6记录']}).to_excel(writer, sheet_name='赠品汇总表', index=False)
                    
                    # 获取上个月
                    today = datetime.now()
                    if today.month == 1:
                        last_month = today.replace(year=today.year - 1, month=12)
                    else:
                        last_month = today.replace(month=today.month - 1)
                    last_month_name = last_month.strftime('%Y年%m月')
                    file_name = f'{last_month_name}美菱IB00工厂盘存数据、账外物资汇总.xlsx'
                    
                    # 提供下载
                    st.success("🎉 匹配完成！")
                    st.download_button(
                        label="📥 下载盘存汇总表",
                        data=output_buffer.getvalue(),
                        file_name=file_name,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                    
                    # 显示预览
                    st.subheader("📊 成品汇总预览")
                    st.dataframe(product_data.head(20), use_container_width=True)
                    
                except Exception as e:
                    st.error(f"❌ 处理失败: {str(e)}")
                    st.exception(e)

    # 使用说明
    with st.expander("📖 使用说明", expanded=False):
        st.markdown("""
        ### 📦 IB00库存匹配功能说明
        
        **操作步骤：**
        1. 上传 **IB00库存表**（包含存储位置、非限制使用库存、冻结库存列）
        2. 上传 **库位表**（包含实物库位表和赠品库位表两个工作表）
        3. 根据需要调整列名配置（通常无需修改）
        4. 点击“开始匹配”按钮
        5. 下载生成的盘存汇总表
        
        **输出内容：**
        - 成品汇总表：按仓库描述汇总的总库存（ERP账面数）
        - 赠品汇总表：库位尾数为6且匹配成功的赠品库存汇总
        
        **注意事项：**
        - 库位表中的库位代码列名和描述列名需与配置一致
        - 赠品库位表工作表名默认为“赠品库位表”，如不存在则会提示
        - 输出文件名自动带上个月份（如2026年03月）
        """)

if __name__ == "__main__":
    main()
