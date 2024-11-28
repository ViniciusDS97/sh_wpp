from flask import Flask, render_template, request
import subprocess

app = Flask(__name__)

# Rota principal para renderizar a página HTML
@app.route('/')
def index():
    return render_template('index.html')

# Rota para executar o script principal
@app.route('/executar', methods=['POST'])
def run_script():
    try:
        # Inicia o script em um subprocesso separado
        subprocess.Popen(['python', './final_app.py'])
        return "Script iniciado com sucesso!"
    except Exception as e:
        return f"Erro ao iniciar o script: {str(e)}"

if __name__ == '__main__':
    app.run(debug=True)

