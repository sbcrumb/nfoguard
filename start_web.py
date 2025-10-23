#!/usr/bin/env python3
"""
NFOGuard Web Interface Starter
Simple script to start web interface using existing config system
"""
import os
import sys
import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

# Import existing configuration
from config.settings import config

# Import existing database and components
from core.database import NFOGuardDatabase

# Import web routes from existing system
from api.web_routes import register_web_routes

# Import authentication system
from api.auth import SimpleAuthMiddleware, AuthSession


def create_web_app() -> FastAPI:
    """Create FastAPI web application"""
    app = FastAPI(
        title="NFOGuard Web Interface",
        description="Web interface for NFOGuard media database management",
        version="2.7.0-web",
        docs_url=None,  # Disable docs in production
        redoc_url=None
    )
    
    return app


def setup_static_files(app: FastAPI) -> None:
    """Mount static file directories"""
    static_path = os.path.join(os.path.dirname(__file__), "nfoguard-web", "static")
    logo_path = os.path.join(os.path.dirname(__file__), "logo")
    
    print(f"🔍 Checking static path: {static_path} (exists: {os.path.exists(static_path)})")
    print(f"🔍 Checking logo path: {logo_path} (exists: {os.path.exists(logo_path)})")
    
    if os.path.exists(static_path):
        app.mount("/static", StaticFiles(directory=static_path), name="static")
        print(f"✅ Mounted static files from: {static_path}")
    else:
        print(f"❌ Static path not found: {static_path}")
    
    if os.path.exists(logo_path):
        app.mount("/logo", StaticFiles(directory=logo_path), name="logo")
        print(f"✅ Mounted logo files from: {logo_path}")
    else:
        print(f"❌ Logo path not found: {logo_path}")
    
    # Serve index.html at root
    @app.get("/")
    async def serve_index():
        index_file = os.path.join(static_path, "index.html")
        if os.path.exists(index_file):
            return FileResponse(index_file)
        else:
            return {"message": "NFOGuard Web Interface", "status": "running"}
    
    # Serve favicon
    @app.get("/favicon.ico")
    async def serve_favicon():
        # Try to serve favicon from logo directory or static files
        favicon_paths = [
            os.path.join(logo_path, "favicon.ico"),
            os.path.join(static_path, "favicon.ico"),
            os.path.join(logo_path, "NFOGuardLogo.png")  # Fallback to new logo
        ]
        
        for favicon_path in favicon_paths:
            if os.path.exists(favicon_path):
                return FileResponse(favicon_path)
        
        # Return 204 No Content if no favicon found
        from fastapi import Response
        return Response(status_code=204)


def main():
    """Main entry point for NFOGuard Web Interface"""
    print("🌐 Starting NFOGuard Web Interface...")
    
    # Use existing config system
    web_host = os.environ.get("WEB_HOST", "0.0.0.0")
    web_port = int(os.environ.get("WEB_PORT", "8081"))
    
    print(f"📊 Configuration: Port {web_port}")
    
    # Create FastAPI app
    app = create_web_app()
    
    # Initialize database using existing system
    try:
        db = NFOGuardDatabase(config)
        print(f"✅ Connected to database: {config.db_host}:{config.db_port}/{config.db_name}")
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        sys.exit(1)
    
    # Setup authentication if enabled
    auth_enabled = getattr(config, 'web_auth_enabled', False)
    session_manager = None
    
    if auth_enabled:
        session_timeout = getattr(config, 'web_auth_session_timeout', 3600)
        session_manager = AuthSession(timeout_seconds=session_timeout)
        print(f"🔐 Web authentication enabled (session timeout: {session_timeout}s)")
    else:
        print("🌐 Web authentication disabled")
    
    # Create dependencies for dependency injection (simplified for web-only)
    dependencies = {
        "db": db,
        "config": config,
        "nfo_manager": None,  # Not needed for read-only web interface
        "movie_processor": None,  # Not needed for read-only web interface  
        "tv_processor": None,  # Not needed for read-only web interface
        "auth_enabled": auth_enabled,
        "session_manager": session_manager
    }
    
    # Add authentication middleware if enabled (BEFORE routes)
    if auth_enabled:
        # Pass the session manager to middleware so it uses the same instance
        app.add_middleware(SimpleAuthMiddleware, config=config, session_manager=session_manager)
        print("🔐 Authentication middleware added to web interface")
    
    # Setup static files and routes
    setup_static_files(app)
    
    # Register web routes
    register_web_routes(app, dependencies)
    
    print(f"🚀 Starting web server on {web_host}:{web_port}")
    
    try:
        uvicorn.run(
            app,
            host=web_host,
            port=web_port,
            workers=1,
            log_level="info",
            access_log=False
        )
    except KeyboardInterrupt:
        print("\n🛑 Web interface shutdown by user")
    except Exception as e:
        print(f"❌ Web interface failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()