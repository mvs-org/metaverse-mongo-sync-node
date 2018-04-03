FROM library/node:8.11.0-alpine

# Set the work directory
RUN mkdir -p /var/www/app
WORKDIR /var/www/app

# Add package.json and install dependencies
COPY package.json ./
RUN npm i

# Install forever globally
RUN npm i -g forever

# Add application files
COPY . /var/www/app

EXPOSE 80

CMD ["forever", "--minUptime", "100", "--spinSleepTime", "10", "index.js"]