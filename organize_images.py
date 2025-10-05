#!/usr/bin/env python3
# organize_images.py
# 整理 downloaded_images 文件夹中的图片，按日期归类

import os
import shutil
from pathlib import Path
from datetime import datetime
import re
import sys

# 配置
IMAGE_SAVE_DIR = Path("./downloaded_images")
SUPPORTED_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp'}

# 统计信息
stats = {
    'total_files': 0,
    'organized_files': 0,
    'already_organized': 0,
    'invalid_files': 0,
    'errors': 0
}

def extract_date_from_filename(filename):
    """
    从文件名中提取日期
    支持的格式：
    - YYYYMMDD_HHMMSS_mmm_xxxxxxxx.ext (当前格式)
    - YYYYMMDD_*.ext
    """
    # 尝试匹配 YYYYMMDD 格式的日期
    date_pattern = r'(\d{8})_'
    match = re.match(date_pattern, filename)
    
    if match:
        date_str = match.group(1)
        # 验证日期格式
        try:
            datetime.strptime(date_str, '%Y%m%d')
            return date_str
        except ValueError:
            return None
    
    return None

def extract_date_from_mtime(filepath):
    """
    从文件修改时间提取日期
    """
    try:
        mtime = os.path.getmtime(filepath)
        date_obj = datetime.fromtimestamp(mtime)
        return date_obj.strftime('%Y%m%d')
    except Exception as e:
        print(f"  ⚠️  无法获取文件修改时间: {e}")
        return None

def is_valid_date_folder(folder_name):
    """
    检查是否是有效的日期文件夹（8位数字，YYYYMMDD格式）
    """
    if not folder_name.isdigit() or len(folder_name) != 8:
        return False
    
    try:
        datetime.strptime(folder_name, '%Y%m%d')
        return True
    except ValueError:
        return False

def organize_images(dry_run=True, verbose=False):
    """
    整理图片文件
    
    Args:
        dry_run: 如果为True，只显示将要执行的操作，不实际移动文件
        verbose: 详细输出模式
    """
    global stats
    
    print("="*60)
    print("📁 图片整理工具")
    print("="*60)
    
    if not IMAGE_SAVE_DIR.exists():
        print(f"❌ 错误: 目录 '{IMAGE_SAVE_DIR}' 不存在")
        return
    
    print(f"📂 扫描目录: {IMAGE_SAVE_DIR.absolute()}")
    print(f"🔍 模式: {'预览模式（不会实际移动文件）' if dry_run else '执行模式（将实际移动文件）'}")
    print()
    
    # 收集需要整理的文件
    files_to_organize = []
    
    # 遍历主目录中的文件
    for item in IMAGE_SAVE_DIR.iterdir():
        if item.is_file():
            stats['total_files'] += 1
            
            # 检查是否是图片文件
            if item.suffix.lower() not in SUPPORTED_EXTENSIONS:
                if verbose:
                    print(f"  ⏭️  跳过非图片文件: {item.name}")
                stats['invalid_files'] += 1
                continue
            
            # 提取日期
            date_str = extract_date_from_filename(item.name)
            
            # 如果文件名中没有日期，使用修改时间
            if not date_str:
                if verbose:
                    print(f"  ℹ️  文件名无日期信息，使用修改时间: {item.name}")
                date_str = extract_date_from_mtime(item)
            
            if date_str:
                files_to_organize.append((item, date_str))
            else:
                print(f"  ⚠️  无法确定日期，跳过: {item.name}")
                stats['invalid_files'] += 1
        
        elif item.is_dir():
            # 检查现有的日期文件夹
            if is_valid_date_folder(item.name):
                folder_files = list(item.glob('*'))
                image_files = [f for f in folder_files if f.suffix.lower() in SUPPORTED_EXTENSIONS]
                stats['already_organized'] += len(image_files)
                if verbose:
                    print(f"  ✅ 已组织的文件夹: {item.name} ({len(image_files)} 个文件)")
            elif verbose:
                print(f"  ℹ️  非日期文件夹: {item.name}")
    
    # 显示统计信息
    print()
    print("📊 扫描统计:")
    print(f"  - 主目录中的文件总数: {stats['total_files']}")
    print(f"  - 需要整理的文件: {len(files_to_organize)}")
    print(f"  - 已在日期文件夹中的文件: {stats['already_organized']}")
    print(f"  - 跳过的文件: {stats['invalid_files']}")
    print()
    
    if not files_to_organize:
        print("✅ 没有需要整理的文件")
        return
    
    # 按日期分组
    date_groups = {}
    for filepath, date_str in files_to_organize:
        if date_str not in date_groups:
            date_groups[date_str] = []
        date_groups[date_str].append(filepath)
    
    print(f"📅 将文件分组到 {len(date_groups)} 个日期文件夹:")
    for date_str in sorted(date_groups.keys()):
        print(f"  - {date_str}: {len(date_groups[date_str])} 个文件")
    print()
    
    # 执行整理
    if dry_run:
        print("🔍 预览将要执行的操作:")
        print()
    else:
        print("🚀 开始整理文件:")
        print()
    
    for date_str, files in sorted(date_groups.items()):
        # 创建日期文件夹
        date_folder = IMAGE_SAVE_DIR / date_str
        
        if dry_run:
            print(f"📁 [{date_str}] 将创建/使用文件夹: {date_folder}")
        else:
            date_folder.mkdir(exist_ok=True)
            print(f"📁 [{date_str}] 文件夹已准备: {date_folder}")
        
        # 移动文件
        for filepath in files:
            target_path = date_folder / filepath.name
            
            # 检查目标文件是否已存在
            if target_path.exists():
                print(f"  ⚠️  文件已存在，跳过: {filepath.name}")
                continue
            
            if dry_run:
                print(f"  ➡️  {filepath.name} -> {date_str}/{filepath.name}")
            else:
                try:
                    shutil.move(str(filepath), str(target_path))
                    print(f"  ✅ {filepath.name} -> {date_str}/{filepath.name}")
                    stats['organized_files'] += 1
                except Exception as e:
                    print(f"  ❌ 移动失败: {filepath.name} - {e}")
                    stats['errors'] += 1
        
        print()
    
    # 最终统计
    print("="*60)
    print("📈 整理完成!")
    print("="*60)
    
    if not dry_run:
        print(f"  ✅ 成功整理: {stats['organized_files']} 个文件")
        print(f"  ⚠️  发生错误: {stats['errors']} 个文件")
    else:
        print(f"  📋 预计整理: {len(files_to_organize)} 个文件")
        print()
        print("💡 提示: 使用 --execute 参数执行实际整理")
    
    print("="*60)

def main():
    """
    主函数
    """
    import argparse
    
    parser = argparse.ArgumentParser(
        description='整理 downloaded_images 文件夹中的图片，按日期归类',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python organize_images.py                  # 预览模式（不实际移动文件）
  python organize_images.py --execute        # 执行整理
  python organize_images.py --execute -v     # 执行整理（详细输出）
        """
    )
    
    parser.add_argument(
        '--execute',
        action='store_true',
        help='实际执行整理（默认为预览模式）'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='详细输出模式'
    )
    
    args = parser.parse_args()
    
    # 确认执行
    if args.execute:
        print()
        response = input("⚠️  确认要执行整理操作吗？这将移动文件到日期文件夹。(y/N): ")
        if response.lower() != 'y':
            print("❌ 操作已取消")
            return
        print()
    
    organize_images(dry_run=not args.execute, verbose=args.verbose)

if __name__ == "__main__":
    main()