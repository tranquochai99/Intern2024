import pandas as pd
import pyodbc
DRIVER = 'SQL Server'
SERVER = 'INT-TQHAI-LAPTO'
DATABASE = 'DBCL'
USERNAME = 'sa'
PASSWORD = '12345'
conn = pyodbc.connect(f'DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};connect_timeout=30')
TenKhoa = ''
DotKhaoSat = ''

sql_query = "select distinct MaDotKS from PHIEUKSHP"
df =  pd.read_sql_query(sql_query, conn)

sql_query_1 = "select TenKhoa from Khoa"
df_1 = pd.read_sql_query(sql_query_1, conn)

def ChuyenNganh(TenKhoa) :
    sql_query_2 = "select TenChuyenNganh from CHUYENNGANH join KHOA on CHUYENNGANH.MaKhoa = KHOA.MaKhoa where TenKhoa = ? "
    df_2 = pd.read_sql_query(sql_query_2, conn, params=TenKhoa)
    return df_2

def Tao_df_1(TenKhoa, DotKhaoSat) :
    sql_query = ('select SINHVIEN.MaLop, HOCPHAN.MaHP, TenHP, (GIANGVIEN.HoTenLot + GIANGVIEN.Ten) as "HoVaTenGV", TenKhoa, LOPHOCPHAN.MaLopHP, '
                            'KQKSHP_NUM.MaCauHoi, KQKSHP_NUM.TraLoi, KQKSHP_TXT.TraLoi '
                    'from KQKSHP_NUM join KQKSHP_TXT on KQKSHP_NUM.MaPhieuKSHP = KQKSHP_TXT.MaPhieuKSHP '
                                    'join PHIEUKSHP on KQKSHP_NUM.MaPhieuKSHP = PHIEUKSHP.MaPhieuKSHP '
                                    'join SINHVIEN on PHIEUKSHP.MaSV = SINHVIEN.MaSV '
                                    'join LOPHOCPHAN on PHIEUKSHP.MaLopHP = LOPHOCPHAN.MaLopHP '
                                    'join HOCPHAN on LOPHOCPHAN.MaHP = HOCPHAN.MaHP '
                                    'join GIANGVIEN on LOPHOCPHAN.GiangVienGD = GIANGVIEN.MaGV '
                                    'join KHOA on GIANGVIEN.MaKhoa = KHOA.MaKhoa '
                    'where TenKhoa = ? and MaDotKS = ?'
                   )
    df = pd.read_sql_query(sql_query, conn, params=[TenKhoa, DotKhaoSat])
    return df

def Tao_df_2(TenKhoa, DotKhaoSat, TenChuyenNganh) :
    sql_query = ('select SINHVIEN.MaLop, HOCPHAN.MaHP, TenHP, (GIANGVIEN.HoTenLot + GIANGVIEN.Ten) as "HoVaTenGV", TenKhoa, LOPHOCPHAN.MaLopHP, '
                            'KQKSHP_NUM.MaCauHoi, KQKSHP_NUM.TraLoi, KQKSHP_TXT.TraLoi '
                    'from KQKSHP_NUM join KQKSHP_TXT on KQKSHP_NUM.MaPhieuKSHP = KQKSHP_TXT.MaPhieuKSHP '
                                    'join PHIEUKSHP on KQKSHP_NUM.MaPhieuKSHP = PHIEUKSHP.MaPhieuKSHP '
                                    'join SINHVIEN on PHIEUKSHP.MaSV = SINHVIEN.MaSV '
                                    'join LOPHOCPHAN on PHIEUKSHP.MaLopHP = LOPHOCPHAN.MaLopHP '
                                    'join HOCPHAN on LOPHOCPHAN.MaHP = HOCPHAN.MaHP '
                                    'join GIANGVIEN on LOPHOCPHAN.GiangVienGD = GIANGVIEN.MaGV '
                                    'join KHOA on GIANGVIEN.MaKhoa = KHOA.MaKhoa '
                                    'join LOP on SINHVIEN.MaLop = LOP.MaLop '
                                    'join CHUYENNGANH on LOP.MaChuyenNganh = CHUYENNGANH.MaChuyenNganh'
                    'where TenKhoa = ? and MaDotKS = ? and TenChuyenNganh = ?'
                   )
    df = pd.read_sql_query(sql_query, conn, params=[TenKhoa, DotKhaoSat, TenChuyenNganh])
    return df