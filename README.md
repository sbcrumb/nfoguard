# NFOGuard

[![Docker Pulls](https://img.shields.io/docker/pulls/sbcrumb/nfoguard.svg)](https://hub.docker.com/r/sbcrumb/nfoguard)
[![Docker Image Version](https://img.shields.io/docker/v/sbcrumb/nfoguard?sort=semver)](https://hub.docker.com/r/sbcrumb/nfoguard)
[![Docker Image Size](https://img.shields.io/docker/image-size/sbcrumb/nfoguard/latest)](https://hub.docker.com/r/sbcrumb/nfoguard)

---

> **⚠️ ALPHA SOFTWARE NOTICE ⚠️**
>
> NFOGuard is currently in **Alpha** stage. While functional, it may have bugs or missing features.
>
> **🔌 Emby Plugin Available**: To take full advantage of NFOGuard's capabilities, join our Discord server to get access to the companion Emby plugin:
>
> **[Join Discord: https://discord.gg/bbD9Pmtr](https://discord.gg/bbD9Pmtr)**
>
> *If the Discord link has expired, please [open an issue](https://github.com/your-username/NFOguard/issues) and we'll provide an updated link.*

---

**Automated NFO file management for Radarr and Sonarr with intelligent date handling**

NFOGuard automatically updates movie and TV show NFO files with proper release dates and metadata when triggered by Radarr/Sonarr webhooks. It preserves existing metadata while adding clean, accurate date information at the bottom of NFO files.

## ✨ Features

- 🎬 **Movie & TV Support** - Works with both Radarr and Sonarr
- 📅 **Smart Date Handling** - Prioritizes digital, physical, and theatrical release dates
- 🔄 **Webhook Integration** - Triggers automatically on import, upgrade, and rename
- 🗄️ **Database Integration** - Direct PostgreSQL access for better performance
- 📝 **NFO Preservation** - Maintains existing metadata, adds fields cleanly at bottom
- 🔒 **Metadata Locking** - Prevents overwrites with lockdata tags
- ⚡ **Batch Processing** - Efficient handling of multiple files
- 🐳 **Docker Ready** - Easy deployment with Docker Compose

## 🚀 Quick Start

### 1. Download Configuration Files

```bash
wget https://raw.githubusercontent.com/sbcrumb/NFOguard/main/.env.template
wget https://raw.githubusercontent.com/sbcrumb/NFOguard/main/.env.secrets.template
wget https://raw.githubusercontent.com/sbcrumb/NFOguard/main/docker-compose.example.yml
```

### 2. Configure Environment

```bash
# Copy and edit main configuration
cp .env.template .env
nano .env

# Copy and edit secrets (API keys, passwords)
cp .env.secrets.template .env.secrets
nano .env.secrets
chmod 600 .env.secrets

# Copy and edit Docker Compose
cp docker-compose.example.yml docker-compose.yml
nano docker-compose.yml
```

### 3. Deploy

```bash
# Create data directory
mkdir -p ./data

# Start NFOGuard
docker-compose up -d

# Check logs
docker-compose logs -f nfoguard

# Verify health
curl http://localhost:8080/health
```

## ⚙️ Configuration

### Environment Files

| File | Purpose | Contains |
|------|---------|----------|
| `.env` | Main configuration | Paths, behavior settings, non-sensitive options |
| `.env.secrets` | Sensitive data | API keys, passwords, database credentials |

### Key Configuration Options

**Media Paths** (Required):
```bash
# Container paths (what NFOGuard sees)
MOVIE_PATHS=/media/Movies/movies,/media/Movies/movies6
TV_PATHS=/media/TV/tv,/media/TV/tv6

# *arr application paths (what your apps see)
RADARR_ROOT_FOLDERS=/mnt/unionfs/Media/Movies/movies
SONARR_ROOT_FOLDERS=/mnt/unionfs/Media/TV/tv
```

**Release Date Priority**:
```bash
RELEASE_DATE_PRIORITY=digital,physical,theatrical
```

**Debug Mode**:
```bash
DEBUG=false                    # Clean production logs
SUPPRESS_TVDB_WARNINGS=true    # Hide non-critical API failures
```

## 🐳 Docker Images

### Production (Stable)
```yaml
image: sbcrumb/nfoguard:latest
```

### Development (Latest Features)
```yaml
image: sbcrumb/nfoguard:dev
```

### Specific Version
```yaml
image: sbcrumb/nfoguard:v1.5.5
```

## 🔗 Webhook Setup

Configure these webhook URLs in your applications:

**Radarr**: `http://nfoguard:8080/webhook/radarr`  
**Sonarr**: `http://nfoguard:8080/webhook/sonarr`

**Triggers**: On Import, On Upgrade, On Rename

## 🔄 Manual Operations

### Manual Scanning

Trigger manual scans via API endpoints:

```bash
# Manual scan all media (movies and TV)
curl -X POST "http://localhost:8080/manual/scan?scan_type=both"

# Manual scan TV only
curl -X POST "http://localhost:8080/manual/scan?scan_type=tv"

# Manual scan movies only
curl -X POST "http://localhost:8080/manual/scan?scan_type=movies"

# Manual scan specific path
curl -X POST "http://localhost:8080/manual/scan?path=/media/movies"

# Bulk update all movies from Radarr database
curl -X POST "http://localhost:8080/bulk/update"
```

### API Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/health` | GET | Health check |
| `/webhook/radarr` | POST | Radarr webhook handler |
| `/webhook/sonarr` | POST | Sonarr webhook handler |
| `/manual/scan` | POST | Manual media scanning |
| `/bulk/update` | POST | Bulk movie updates from Radarr DB |

### Manual Scan Parameters

- `scan_type`: `both`, `movies`, `tv`
- `path`: Specific directory path to scan
- Use for initial setup or fixing existing media

## 📁 Volume Mapping

Critical: Update `docker-compose.yml` with your actual paths:

```yaml
volumes:
  - ./data:/app/data                           # NFOGuard data
  - /your/movies/path:/media/Movies/movies:rw  # Movie library
  - /your/tv/path:/media/TV/tv:rw              # TV library
```

### Common Examples

**Unraid**:
```yaml
- /mnt/user/Media/Movies:/media/Movies/movies:rw
- /mnt/user/Media/TV:/media/TV/tv:rw
```

**Synology**:
```yaml
- /volume1/Media/Movies:/media/Movies/movies:rw
- /volume1/Media/TV:/media/TV/tv:rw
```

**Standard Linux**:
```yaml
- /home/user/media/movies:/media/Movies/movies:rw
- /home/user/media/tv:/media/TV/tv:rw
```

## 🔧 Troubleshooting

### Check Logs
```bash
docker-compose logs -f nfoguard
```

### Enable Debug Mode
```bash
# In .env file
DEBUG=true
PATH_DEBUG=true
```

### Health Check
```bash
curl http://localhost:8080/health
```

### Common Issues

1. **Permission Errors**: Ensure NFOGuard can write to mounted directories
2. **Path Mapping**: Verify container paths match `.env` configuration
3. **Webhooks**: Check URLs and ensure port 8080 is accessible
4. **Database**: Verify PostgreSQL credentials in `.env.secrets`

## 📊 What NFOGuard Does

### Before
```xml
<movie>
  <title>Movie Title</title>
  <year>2023</year>
  <!-- Existing Radarr metadata -->
</movie>
```

### After
```xml
<movie>
  <title>Movie Title</title>
  <year>2023</year>
  <!-- Existing Radarr metadata preserved -->
  
  <!-- NFOGuard additions at bottom -->
  <digital_release_date>2023-03-15</digital_release_date>
  <lockdata>true</lockdata>
  <!-- Manager: NFOGuard -->
</movie>
```

## 📋 Requirements

- Docker and Docker Compose
- Radarr and/or Sonarr
- Media files in accessible directories
- Network connectivity between services

## 🆘 Support

- **Issues**: [GitHub Issues](https://github.com/sbcrumb/NFOguard/issues)
- **Documentation**: See `SETUP_GUIDE.md` for detailed instructions
- **Docker Hub**: [`sbcrumb/nfoguard`](https://hub.docker.com/r/sbcrumb/nfoguard)

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

Commercial licensing and enterprise features may be available separately. Contact us for more information.

---

**NFOGuard** - Keeping your media metadata clean and organized! 🎯
