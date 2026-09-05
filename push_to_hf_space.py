#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hugging Face Spaces Uploader Script
Tu dong tao Space va upload toan bo ma nguon Converter_Tool len Hugging Face.
"""

import os
import sys
import argparse

# Dam bao in tieng Viet tren Windows console khong loi
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except AttributeError:
        pass

from huggingface_hub import HfApi, login

def deploy(token=None, space_name="presto-spark-converter", private=False):
    if token:
        login(token=token, add_to_git_credential=True)
    
    api = HfApi()
    try:
        user_info = api.whoami()
        username = user_info["name"]
        print(f"[OK] Da dang nhap tai khoan Hugging Face: {username}")
    except Exception as e:
        print("[Loi] Ban chua dang nhap Hugging Face!")
        print("Vui long lay User Access Token (role Write) tai: https://huggingface.co/settings/tokens")
        print("Sau do chay: python push_to_hf_space.py --token <YOUR_TOKEN>")
        return False

    repo_id = f"{username}/{space_name}"
    print(f"[*] Dang khoi tao Space: {repo_id} (SDK: Docker)...")

    try:
        api.create_repo(
            repo_id=repo_id,
            repo_type="space",
            space_sdk="docker",
            private=private,
            exist_ok=True
        )
        print(f"[OK] Space da san sang: https://huggingface.co/spaces/{repo_id}")
    except Exception as e:
        print(f"[Loi tao repo]: {e}")
        return False

    current_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"[*] Dang tai len toan bo files tu: {current_dir} ...")

    try:
        api.upload_folder(
            folder_path=current_dir,
            repo_id=repo_id,
            repo_type="space",
            ignore_patterns=[
                "auth.db",
                "*.bat",
                ".git",
                ".git/*",
                "__pycache__",
                "__pycache__/*",
                "*.pyc"
            ],
            commit_message="Deploy Presto <-> Spark SQL Transpiler with Auth"
        )
        print("\n" + "=" * 65)
        print(f"TRIEN KHAI THANH CONG LEN HUGGING FACE SPACES!")
        print(f"-> Link quan ly Space: https://huggingface.co/spaces/{repo_id}")
        print(f"-> Link Web Direct URL: https://{username}-{space_name}.hf.space")
        print("=" * 65)
        return True
    except Exception as e:
        print(f"[Loi upload]: {e}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Deploy to Hugging Face Spaces")
    parser.add_argument("--token", type=str, help="Hugging Face User Access Token")
    parser.add_argument("--name", type=str, default="presto-spark-converter", help="Ten Space")
    parser.add_argument("--private", action="store_true", help="Che do Private cho Space")
    args = parser.parse_args()

    deploy(token=args.token, space_name=args.name, private=args.private)
