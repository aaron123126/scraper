# Web Archiver & Viewer

This project is a comprehensive solution for scraping, archiving, and viewing websites. It consists of two main components: a powerful Python-based web scraper and a modern Node.js server for viewing the archives.

## Features

### Scraper (Python)

- **Asynchronous Scraping**: High-performance crawling using `aiohttp` and `asyncio`.
- **Configurable**: Control crawl depth, max pages, pages per domain, and more via a `.env` file.
- **Polite**: Respects `robots.txt` and has configurable request delays.
- **Asset Handling**: Scrapes HTML, CSS, JavaScript, and images.
- **Optimization**: Optimizes images and minifies text-based assets before archiving.
- **Compression**: Archives scraped sites into compressed `.tar.gz` files for efficient storage.

### Viewer (Node.js)

- **Archive Browser**: A web interface to browse and view the contents of your web archives.
- **Efficient Serving**: Serves content directly from the compressed archives, with a caching layer for performance.
- **Real-time Updates**: Uses Socket.IO for real-time communication.
- **Modern Stack**: Built with Express, Handlebars, and other modern libraries.
- **Security**: Includes security best practices using Helmet and CORS.

## Tech Stack

- **Scraper (Python)**:
  - `aiohttp`: Asynchronous HTTP client/server.
  - `beautifulsoup4`: For parsing HTML and XML.
  - `Pillow`: Image processing library.
  - `python-dotenv`: For managing environment variables.

- **Viewer (Node.js)**:
  - `express`: Web framework for Node.js.
  - `express-handlebars`: View engine for Express.
  - `socket.io`: Real-time, bidirectional event-based communication.
  - `tar`: For reading TAR archives.
  - `helmet`: Helps secure Express apps by setting various HTTP headers.

## Project Structure

```
/
├───.env                # Environment variables for configuration
├───main.py             # Main entry point for the Python scraper
├───scraper.py          # Core web scraping logic
├───compressor.py       # Compresses scraped files into an archive
├───optimizer.py        # Optimizes assets (images, CSS, JS)
├───requirements.txt    # Python dependencies
├───server.js           # Main entry point for the Node.js viewer server
├───package.json        # Node.js dependencies
├───src/                # Node.js server source code
│   ├───app.js          # Express application setup
│   ├───controllers/    # Request handlers
│   ├───routes/         # API and viewer routes
│   ├───services/       # Business logic for the viewer
│   └───utils/          # Utility functions
├───public/             # Static assets for the viewer frontend
└───views/              # Handlebars templates for the viewer
```

## Setup and Installation

### Prerequisites

- Python 3.7+
- Node.js v18+
- `pip` and `npm`

### Configuration

1.  Create a `.env` file in the root of the project.
2.  Add the following configuration variables:

    ```env
    # --- Scraper Configuration ---
    START_URL="https://example.com"  # The initial URL to start scraping from
    MAX_DEPTH=3                      # Maximum crawl depth
    MAX_PAGES=100                    # Maximum number of pages to scrape
    PAGES_PER_DOMAIN=50              # Maximum pages to scrape from a single domain
    MAX_WORKERS=10                   # Number of concurrent scraping workers
    REQUEST_DELAY=0.5                # Delay in seconds between requests to the same domain
    RESPECT_ROBOTS_TXT=true          # Whether to respect robots.txt rules
    SKIP_ASSETS=false                # If true, skips scraping of CSS, JS, and images

    # --- Optimizer & Compressor Configuration ---
    IMAGE_QUALITY=85                 # Image quality for optimization (1-100)
    MAX_IMAGE_WIDTH=1920             # Maximum width for resized images
    COMPRESSION_LEVEL=19             # Brotli compression level for the final archive

    # --- Directory Configuration ---
    OUTPUT_DIR="./scraped_data"      # Directory to save scraped data
    ARCHIVE_DIR="./archives"         # Directory to save compressed archives

    # --- Server Configuration ---
    PORT=3000                        # Port for the Node.js viewer server
    HOST="localhost"                 # Host for the Node.js server
    ```

### Installation

1.  **Python Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Node.js Dependencies**:
    ```bash
    npm install
    ```

## Usage

### 1. Scraping and Archiving

To start the web scraping and archiving process, run `main.py`:

```bash
python main.py
```

The script will use the configuration from your `.env` file, scrape the target website, and create a compressed archive in the `archives` directory.

### 2. Viewing Archives

To start the web viewer, run `server.js`:

```bash
npm start
```

You can then access the viewer in your browser at `http://localhost:3000` (or the host and port you configured).

## Contributing

Contributions are welcome! Please feel free to submit a pull request.

1.  Fork the repository.
2.  Create your feature branch (`git checkout -b feature/AmazingFeature`).
3.  Commit your changes (`git commit -m 'Add some AmazingFeature'`).
4.  Push to the branch (`git push origin feature/AmazingFeature`).
5.  Open a pull request.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.
