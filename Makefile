.PHONY: all install clean uninstall

# Build binary
all: statka

statka:
	go build -o statka main.go

# Install as systemd service
install: statka
	# Create directories
	sudo mkdir -p /opt/statka
	sudo cp statka /opt/statka/
	sudo cp statka.json /opt/statka/
	sudo chmod +x /opt/statka/statka

	# Create systemd service
	sudo cp systemd/statka.service /etc/systemd/system/
	sudo systemctl daemon-reload
	sudo systemctl enable statka
	sudo systemctl start statka

	@echo "✅ Statka installed and started!"
	@echo "📊 Logs: journalctl -u statka -f"
	@echo "🔍 Status: systemctl status statka"

# Clean build artifacts
clean:
	rm -f statka
