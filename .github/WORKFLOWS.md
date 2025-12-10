# GitHub Actions CI/CD Setup

This repository uses GitHub Actions for continuous integration and deployment.

## Workflows

### 1. Docker Build and Push (`docker-build.yml`)
**Trigger**: Push to `main` branch

**What it does**:
- Builds Docker image from Dockerfile
- Pushes to DockerHub with multiple tags:
  - `latest` (for main branch)
  - `main-<sha>` (commit-specific)
  - `main` (branch name)
- Uses GitHub Actions cache for faster builds

**Required Secrets**:
- `DOCKERHUB_USERNAME`: Your DockerHub username
- `DOCKERHUB_TOKEN`: DockerHub access token (create at https://hub.docker.com/settings/security)

### 2. Run Tests (`test.yml`)
**Trigger**: Pull request to `main` branch

**What it does**:
- Runs all unit tests in the `tests/` directory
- Tests against Python versions: 3.10, 3.11, 3.12
- Uses pip caching for faster builds
- Displays test results in PR checks

**No secrets required**

### 3. Code Formatting Check (`black.yml`)
**Trigger**: Pull request to `main` branch

**What it does**:
- Validates Python code formatting using Black
- Shows diff of formatting issues if any
- Provides helpful error message on failure

**No secrets required**

## Setup Instructions

### 1. Configure DockerHub Secrets

To enable Docker image builds, add these secrets to your GitHub repository:

1. Go to your repository on GitHub
2. Navigate to **Settings** → **Secrets and variables** → **Actions**
3. Click **New repository secret**
4. Add the following secrets:
   - Name: `DOCKERHUB_USERNAME`, Value: Your DockerHub username
   - Name: `DOCKERHUB_TOKEN`, Value: Your DockerHub access token

To create a DockerHub access token:
1. Log in to [DockerHub](https://hub.docker.com)
2. Go to **Account Settings** → **Security**
3. Click **New Access Token**
4. Give it a description (e.g., "GitHub Actions")
5. Copy the token and use it as `DOCKERHUB_TOKEN`

### 2. Update DockerHub Image Name (Optional)

If your DockerHub username is different from the default, update line 22 in `.github/workflows/docker-build.yml`:

```yaml
images: ${{ secrets.DOCKERHUB_USERNAME }}/logstar-online-stream
```

Change `logstar-online-stream` to your desired image name.

### 3. Format Code with Black

Before creating a PR, ensure your code is formatted:

```bash
# Install Black
pip install black

# Format all Python files
black .

# Check formatting without making changes
black --check .
```

## Workflow Status

You can view the status of workflows:
- In the **Actions** tab of your GitHub repository
- As checks on pull requests
- As badges (add to README.md):

```markdown
![Docker Build](https://github.com/YOUR_USERNAME/Logstar-online-Stream/actions/workflows/docker-build.yml/badge.svg)
![Tests](https://github.com/YOUR_USERNAME/Logstar-online-Stream/actions/workflows/test.yml/badge.svg)
![Black](https://github.com/YOUR_USERNAME/Logstar-online-Stream/actions/workflows/black.yml/badge.svg)
```

## Troubleshooting

### Docker Build Fails
- Verify `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` secrets are set correctly
- Check that your DockerHub account has permissions to push images
- Ensure the Dockerfile is valid

### Tests Fail
- Run tests locally: `python -m unittest discover tests -v`
- Check if all dependencies are in `requirements.txt`
- Verify Python version compatibility

### Black Formatting Fails
- Run `black .` locally to format code
- Run `black --check --diff .` to see what needs formatting
- Commit the formatted code

## Local Testing

Test workflows locally before pushing:

```bash
# Run tests
python -m unittest discover tests -v

# Check Black formatting
black --check .

# Build Docker image
docker build -t logstar-online-stream:test .
```
