# fix_app_imports.py
import os

PROJECT_ROOT = os.path.join(os.getcwd(), 'flask_app')
REPLACE_FROM = 'from app.'
REPLACE_WITH = 'from flask_app.app.'

EXCLUDED_FOLDERS = {'local archive', '.ipynb_checkpoints'}

def fix_imports():
    print("🛠 Replacing 'from app.' with 'from flask_app.app.'...\n")
    for foldername, subfolders, filenames in os.walk(PROJECT_ROOT):
        # Skip excluded folders
        if any(excluded in foldername for excluded in EXCLUDED_FOLDERS):
            continue

        for filename in filenames:
            if filename.endswith('.py'):
                filepath = os.path.join(foldername, filename)
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read()

                if REPLACE_FROM in content:
                    new_content = content.replace(REPLACE_FROM, REPLACE_WITH)
                    with open(filepath, 'w', encoding='utf-8') as f:
                        f.write(new_content)
                    print(f"✅ Fixed: {filepath}")
                else:
                    print(f"⏭️  Skipped (no change): {filepath}")

    print("\n✅ All applicable files updated (excluding archived/checkpoint files).")

if __name__ == "__main__":
    fix_imports()
