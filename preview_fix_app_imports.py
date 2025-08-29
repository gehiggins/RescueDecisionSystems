# preview_fix_app_imports.py
import os

PROJECT_ROOT = os.path.join(os.getcwd(), 'flask_app')
REPLACE_FROM = 'from app.'
REPLACE_WITH = 'from flask_app.app.'

def preview_import_fixes():
    print("🔍 Scanning for 'from app.' import statements...\n")
    for foldername, subfolders, filenames in os.walk(PROJECT_ROOT):
        for filename in filenames:
            if filename.endswith('.py'):
                filepath = os.path.join(foldername, filename)
                with open(filepath, 'r', encoding='utf-8') as f:
                    lines = f.readlines()

                found = False
                for i, line in enumerate(lines):
                    if REPLACE_FROM in line:
                        if not found:
                            print(f"\n📄 File: {filepath}")
                            found = True
                        print(f"  Line {i+1}: {line.strip()}")
                        print(f"   👉 Would become: {line.replace(REPLACE_FROM, REPLACE_WITH).strip()}")

    print("\n✅ Preview complete. No files were changed.")

if __name__ == "__main__":
    preview_import_fixes()
