FROM scratch
COPY ./antithesis/config/docker-compose.yml docker-compose.yaml
COPY ./antithesis/config/environment .env

# Install version and buildinfo files
COPY ./antithesis/config/version /opt/antithesis/version
COPY ./antithesis/config/buildinfo /opt/antithesis/buildinfo
