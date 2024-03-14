from flask import Flask, render_template, request
import subprocess

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload():
    source_file_path = request.form['source_file_path']
    target_path = request.form['target_path']
    pem_file_path = request.form['pem_file_path']
    client_id = request.form['client_id']
    client_storage_account_name = request.form['client_storage_account_name']
    
    command = f'python script.py {source_file_path} {target_path} {pem_file_path} {client_id} {client_storage_account_name}'
    subprocess.Popen(command, shell=True)
    
    return 'File upload initiated.'

if __name__ == '__main__':
    app.run(debug=True)