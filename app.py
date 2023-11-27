from flask import Flask, render_template

app = Flask(__name__)

@app.route('/')
def index():
    # Pasa el código de Canva como una variable a la plantilla
    canva_code = '<div style="position: relative; width: 100%; height: 0; padding-top: 56.2500%; ...">'
    canva_code += '<iframe loading="lazy" style="position: absolute; width: 100%; height: 100%; top: 0; left: 0; ...">'
    canva_code += '<a href="https://www.canva.com/design/DAF1YnNoS1A/yC3oxB-O2qRw9BxV6LCY1A/edit?ui=eyJHIjp7fX0 " target="_blank" rel="noopener">Pitch_Equipo 9</a>'
    canva_code += '</iframe></div>'

    return render_template('index.html', canva_code=canva_code)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000, debug=True)

