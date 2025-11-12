# Deployment Guide for KEMET Data Explorer

## Quick Start: Deploy to Render.com

### Prerequisites

1. GitHub repository with the code (excluding large database files)
2. Database files hosted externally (see options below)
3. Render.com account (free tier available)

### Step 1: Host Database Files Externally

Since the database files are too large for git (741 MB + 84 MB), you need to host them separately.

#### Option A: Zenodo (Recommended for Academic Projects)

1. Create account at https://zenodo.org
2. Click "New Upload"
3. Upload `corpus.duckdb` and `lexicon.duckdb`
4. Publish and get DOI
5. Copy the direct download URLs (format: `https://zenodo.org/records/XXXXX/files/corpus.duckdb`)

#### Option B: GitHub Releases

1. Create a new release in your GitHub repository
2. Attach `corpus.duckdb` and `lexicon.duckdb` as release assets
3. Get direct download URLs (format: `https://github.com/tresoldi/kemet-data/releases/download/v1.0/corpus.duckdb`)

#### Option C: Google Drive / Dropbox

1. Upload files to Google Drive or Dropbox
2. Generate public shareable links
3. Convert to direct download URLs:
   - Google Drive: Use format `https://drive.google.com/uc?export=download&id=FILE_ID`
   - Dropbox: Replace `?dl=0` with `?dl=1`

#### Option D: Object Storage (AWS S3, DigitalOcean Spaces, etc.)

1. Upload files to S3/Spaces bucket
2. Make files publicly accessible
3. Use the direct URLs

### Step 2: Update Configuration

Edit `web/assets/js/config.js` to point to your external database URLs:

```javascript
export const config = {
    // Database paths - UPDATE THESE WITH YOUR URLS
    databases: {
        corpus: 'https://zenodo.org/records/XXXXX/files/corpus.duckdb',
        lexicon: 'https://zenodo.org/records/XXXXX/files/lexicon.duckdb'
    },

    // Keep the rest of the config as is
    versions: {
        corpus: '1.0.0',
        lexicon: '1.0.0'
    },
    // ...
};
```

### Step 3: Push to GitHub

```bash
# Make sure database files are in .gitignore
git add .
git commit -m "Configure for external database hosting"
git push origin main
```

### Step 4: Deploy on Render.com

1. **Sign in to Render**: Go to https://render.com and sign in
2. **New Static Site**: Click "New +" → "Static Site"
3. **Connect Repository**:
   - Connect your GitHub account
   - Select `kemet-data` repository
4. **Configure Build**:
   - **Name**: `kemet-data-explorer`
   - **Branch**: `main`
   - **Root Directory**: `web`
   - **Build Command**: (leave empty)
   - **Publish Directory**: `.` (current directory)
5. **Create Static Site**: Click "Create Static Site"
6. **Wait for Deployment**: Takes ~1 minute
7. **Access**: Your site will be live at `https://kemet-data-explorer.onrender.com`

### Step 5: Custom Domain (Optional)

1. In Render dashboard, go to your static site
2. Click "Settings" → "Custom Domain"
3. Add your domain (e.g., `kemet.example.com`)
4. Update DNS records as instructed by Render
5. SSL certificate is automatically provisioned

## Alternative: Deploy to GitHub Pages

If you prefer GitHub Pages:

1. **Host databases externally** (same as above)
2. **Update config.js** with external URLs
3. **Enable GitHub Pages**:
   ```bash
   # Option 1: Use gh-pages branch
   npm install -g gh-pages
   gh-pages -d web

   # Option 2: Use docs/ directory
   cp -r web/ docs/
   git add docs/
   git commit -m "Add docs for GitHub Pages"
   git push
   # Then enable Pages in Settings → Pages → Source: docs/
   ```

## Alternative: Deploy to Netlify

1. **Sign in to Netlify**: https://netlify.com
2. **New site from Git**: Select your repository
3. **Build settings**:
   - Base directory: `web`
   - Build command: (leave empty)
   - Publish directory: `web`
4. **Deploy site**

## CORS Considerations

If you host databases on a different domain than your web interface, you need CORS headers.

### Zenodo
✅ CORS enabled by default - works out of the box

### GitHub Releases
✅ CORS enabled by default - works out of the box

### AWS S3
Add CORS configuration to your bucket:
```json
[
    {
        "AllowedHeaders": ["*"],
        "AllowedMethods": ["GET", "HEAD"],
        "AllowedOrigins": ["*"],
        "ExposeHeaders": ["Content-Length", "Content-Type"]
    }
]
```

### Custom Server
Add these headers:
```
Access-Control-Allow-Origin: *
Access-Control-Allow-Methods: GET, HEAD
Access-Control-Allow-Headers: Content-Type
```

## Performance Tips

### CDN (Optional but Recommended)

For faster database downloads, use a CDN:
- Cloudflare (free tier available)
- Fastly
- AWS CloudFront

### Caching Headers

Ensure your database hosting includes:
```
Cache-Control: public, max-age=31536000
```

This allows browsers to cache the databases for 1 year.

## Monitoring

### Render.com

- View logs: Dashboard → Your Site → Logs
- Analytics: Dashboard → Your Site → Analytics
- Custom domain SSL: Automatic

### Health Check

Add a simple health check endpoint (optional):
```javascript
// In web/health.json
{
  "status": "ok",
  "version": "1.0.0"
}
```

## Troubleshooting

### Database Download Fails

**Symptom**: Loading screen stuck at "Downloading..."

**Solutions**:
1. Check browser console for CORS errors
2. Verify database URLs are accessible (open in new tab)
3. Check if hosting service is down
4. Try alternative hosting (Zenodo → GitHub Releases)

### Out of Memory on Render

**Symptom**: Build fails with memory error

**Solution**: Static sites on Render don't build anything, just serve files. If you see memory issues:
1. Ensure `Build Command` is empty
2. Check that databases are NOT in the git repository
3. Verify `.gitignore` excludes `*.duckdb` files

### Slow Initial Load

**Symptom**: First visit takes 5+ minutes

**Solutions**:
1. Use CDN for database hosting
2. Add caching headers
3. Consider compressing databases (though DuckDB files are already compressed)

## Cost Estimates

### Free Tier Options

- **Render.com**: Free static site hosting, 100 GB bandwidth/month
- **Netlify**: Free tier, 100 GB bandwidth/month
- **GitHub Pages**: Free, soft bandwidth limit of 100 GB/month
- **Zenodo**: Free, unlimited bandwidth for research data

### Paid Options (if needed)

- **Render.com Pro**: $7/month, 1 TB bandwidth
- **Netlify Pro**: $19/month, 400 GB bandwidth
- **AWS S3 + CloudFront**: ~$5-10/month for 100 GB transfer

### Bandwidth Calculation

Per user visit:
- First visit: ~825 MB download (corpus 741 MB + lexicon 84 MB)
- Return visits: ~0 MB (cached in browser)

For 100 GB monthly bandwidth:
- ~121 unique visitors per month (first-time downloads)
- Unlimited return visits (cached)

## Production Checklist

Before going live:

- [ ] Database files hosted externally with CORS enabled
- [ ] config.js updated with production database URLs
- [ ] .gitignore excludes all database files
- [ ] Repository pushed to GitHub
- [ ] Static site deployed on Render/Netlify/Pages
- [ ] Custom domain configured (optional)
- [ ] SSL certificate active (automatic on Render/Netlify)
- [ ] Test initial database download
- [ ] Test cached loading on second visit
- [ ] All query templates work
- [ ] Export functions work (CSV/JSON)
- [ ] Links to external resources work (TLA, CDO, etc.)

## Updating the Database

When you have new database versions:

1. Upload new `corpus.duckdb` and `lexicon.duckdb` to your hosting
2. Update version numbers in `config.js`:
   ```javascript
   versions: {
       corpus: '1.1.0',  // Increment version
       lexicon: '1.1.0'
   }
   ```
3. Commit and push
4. Render will auto-deploy
5. Users will automatically download new versions on next visit

## Support

For issues:
- Check browser console (F12) for errors
- Review this guide's troubleshooting section
- Open issue on GitHub: https://github.com/tresoldi/kemet-data/issues
