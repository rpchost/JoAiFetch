# JoAI Deployment Guide for PythonAnywhere

## 🚀 Deployment Steps

### 1. **Initial Setup**
```bash
# In PythonAnywhere console
git clone https://github.com/yourusername/JoAI.git
cd JoAI
python3.10 pythonanywhere_setup.py
```

### 2. **Upload Model Files**
Upload these files to `~/JoAI/models/` via PythonAnywhere file manager:
- `saved_model.keras`
- `saved_model_scaler.pkl`
- `saved_model_target_scaler.pkl`

### 3. **Configure Web App**
- Go to PythonAnywhere Dashboard → Web
- Set **Source code** to: `/home/rpchost/JoAI`
- Set **Working directory** to: `/home/rpchost/JoAI`
- Set **WSGI configuration file** to: `/home/rpchost/JoAI/pythonanywhere_wsgi.py`
- Set **Python version** to: `3.10`

### 4. **Update WSGI Configuration**
Edit `pythonanywhere_wsgi.py` and update your MySQL password:
```python
os.environ.setdefault('MYSQL_PASSWORD', 'your_actual_mysql_password')
```

### 5. **Reload Web App**
Click **Reload** in PythonAnywhere Web dashboard.

## 🧪 Testing Endpoints

### Health Check
```bash
curl https://rpchost.pythonanywhere.com/
```

### NLP Prediction
```bash
curl -X POST https://rpchost.pythonanywhere.com/nlp_predict \
  -H "Content-Type: application/json" \
  -d '{"query": "predict BTC for next hour", "user_id": "test123"}'
```

### Direct Prediction
```bash
curl -X POST https://rpchost.pythonanywhere.com/predict \
  -H "Content-Type: application/json" \
  -d '{"symbol": "BTCUSD", "timeframe": "1h"}'
```

### View Logs
```bash
curl https://rpchost.pythonanywhere.com/logs/html
```

## 🔧 Troubleshooting

### Common Issues:

1. **TensorFlow Import Error**
    - PythonAnywhere has limited TensorFlow support
    - Consider using a lighter ML library or cloud ML service

2. **Memory Limits**
    - PythonAnywhere free tier has 512MB limit
    - LSTM models may exceed memory limits
    - Consider using smaller models or cloud deployment

3. **Database Connection**
    - Ensure MySQL credentials are correct in `pythonanywhere_wsgi.py`
    - Database name should be `username$database_name`

4. **Model Loading**
    - Ensure model files are uploaded to correct path
    - Check file permissions: `chmod 644 models/*`

### Alternative Deployment Options:

#### **Render (Recommended for PostgreSQL)**
- **Database**: Native PostgreSQL support with Render PostgreSQL
- **Setup**: One-click database creation, automatic connection strings
- **Free Tier**: 1GB PostgreSQL database included
- **Scaling**: Automatic scaling with web service
- **Environment**: Container-based, matches local Docker setup

#### **Railway**
- Better for ML workloads with persistent storage

#### **DigitalOcean App Platform**
- More resources available

#### **Google Cloud Run**
- Serverless with GPU support

## 📊 Monitoring

- Check error logs: PythonAnywhere → Web → Error log
- Monitor resource usage: PythonAnywhere → Account → CPU usage
- View access logs: PythonAnywhere → Web → Server log

## 🔄 Updating Deployment

```bash
# In PythonAnywhere console
cd ~/JoAI
git pull origin main
# Reload web app via dashboard
```

## 📞 Support

If you encounter issues:
1. Check PythonAnywhere error logs
2. Verify all environment variables are set
3. Test locally first before deploying
4. Consider upgrading to paid PythonAnywhere plan for more resources