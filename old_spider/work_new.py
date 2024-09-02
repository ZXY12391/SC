from flask import Flask, request, jsonify
import subprocess
import socket

app = Flask(__name__)

# 设置 shell 脚本的路径
SCRIPT_PATH2 = '/data1/zxy/old_spider/test.sh'

@app.route('/run_tweet', methods=['POST'])
def run_tweet():
    try:
        data = request.json
        # 获取 JSON 数据
        trigger = data.get('trigger')
        keyword_list = data.get('keyword_list')
        start_date = data.get('start_date')
        num_months = data.get('num_months')

        if trigger != 'yes':
            return jsonify({'status': 'error', 'message': 'Trigger not set to "yes"'})

        if not all([keyword_list, start_date, num_months]):
            return jsonify({'status': 'error', 'message': 'Missing parameters'})

        # 构建命令
        command = ['bash', SCRIPT_PATH2, keyword_list, start_date, str(num_months)]

        # 使用 subprocess 运行 shell 脚本
        result = subprocess.run(command, capture_output=True, text=True)

        # 获取输出和错误信息
        output = result.stdout
        error = result.stderr

        # 返回结果
        return jsonify({
            'status': 'success',
            'output': output,
            'error': error
        })

    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        })

if __name__ == '__main__':
    hostname = socket.gethostname()
    local_ip = socket.gethostbyname(hostname)
    print(f" * Server running on: http://{local_ip}:5000")
    app.run(host='0.0.0.0', port=5000)
