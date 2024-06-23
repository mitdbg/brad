#include <iostream>
#include <stdexcept>
#include <csignal>
#include <functional>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

#include <gflags/gflags.h>

DEFINE_int32(port, 31337, "Port that this server should listen on.");

DEFINE_int32(proxy_to_port, 5439, "Port that this server should proxy its connection to.");
DEFINE_string(proxy_to_host, "", "The host that this server should proxy its connection to.");

namespace {

class Socket {
 public:
  static Socket Connect(const std::string& host, const uint16_t port) {
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
      perror("Socket failed.");
      throw std::runtime_error("Socket failed.");
    }

    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);

    if(inet_pton(AF_INET, host.c_str(), &serv_addr.sin_addr) <= 0) {
      perror("Host conversion.");
      throw std::runtime_error("Host conversion.");
    }

    if (connect(fd, reinterpret_cast<struct sockaddr *>(&serv_addr), sizeof(serv_addr)) < 0) {
      perror("Connect failed");
      throw std::runtime_error("Connect failed.");
    }

    return Socket(fd);
  }

  // No copying or copy assignment.
  Socket(const Socket&) = delete;
  Socket& operator=(const Socket&) = delete;

  ~Socket() { close(fd_); }

  int fd() const { return fd_; }

 private:
  friend class ServerSocket;
  explicit Socket(int fd) : fd_(fd) {}

  int fd_;
};

class ServerSocket {
 public:
  explicit ServerSocket(uint16_t port) : port_(port), fd_(-1) {
    struct sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);
    
    // Creating socket file descriptor
    fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ == 0) {
      perror("Socket failed");
      throw std::runtime_error("Socket failed");
    }
    
    if (setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
      perror("setsockopt");
      throw std::runtime_error("setsockopt");
    }
    
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);
    
    if (bind(fd_, reinterpret_cast<struct sockaddr *>(&address), sizeof(address)) < 0) {
      perror("bind failed");
      throw std::runtime_error("bind failed");
    }
    
    if (listen(fd_, 1) < 0) {
        perror("listen");
        throw std::runtime_error("listen failed");
    }
  }

  Socket Accept() const {
    struct sockaddr_in address;
    socklen_t addrlen;
    const int new_fd = accept(fd_, reinterpret_cast<struct sockaddr *>(&address), &addrlen);
    if (new_fd < 0) {
      perror("Accept failed");
      throw std::runtime_error("Accept failed");
    }
    return Socket(new_fd);
  }

  ~ServerSocket() { close(fd_); }

  // No copying or copy assignment.
  ServerSocket(const ServerSocket&) = delete;
  ServerSocket& operator=(const ServerSocket&) = delete;

  int fd() const { return fd_; }

 private:
  uint16_t port_;
  int fd_;
};

class SentinelPipe {
 public:
  SentinelPipe() {
    if (pipe(fd_) < 0) {
      perror("Pipe failed.");
      throw std::runtime_error("Pipe failed");
    }
  }

  ~SentinelPipe() {
    for (int i = 0; i < 2; ++i) {
      if (fd_[i] > 0) {
        close(fd_[i]);
        fd_[i] = -1;
      }
    }
  }

  SentinelPipe(const SentinelPipe&) = delete;
  SentinelPipe& operator=(const SentinelPipe&) = delete;

  int read_fd() const { return fd_[0]; }
  int write_fd() const { return fd_[1]; }

 private:
  int fd_[2];
};

class Buffer {
 public:
  Buffer(size_t size) : buf_(nullptr) {
    buf_ = new uint8_t[size];
  }

  ~Buffer() {
    if (buf_ == nullptr) return;
    delete buf_;
    buf_ = nullptr;
  }

  uint8_t* buffer() const { return buf_; }

 private:
  uint8_t* buf_;
};

std::function<void(int)> g_handle_signal;

void signal_wrapper(int signal) {
  if (!g_handle_signal) return;
  g_handle_signal(signal);
}

}  // namespace

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage("Proxies TCP connections.");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_proxy_to_host.empty()) {
    std::cerr << "ERROR: Must provide a value for --proxy-to-host" << std::endl;
    return 1;
  }

  // Workflow:
  // - Start a socket listening for connections on `port`
  // - Once we accept one connection, open a socket to the proxied-to host/port
  // - Shuffle bytes to and from the two connections
  // - Close the sockets on Ctrl-C or when there is an EOF

  ServerSocket server(FLAGS_port);
  std::cerr << "Listening for a connection on port " << FLAGS_port << std::endl;

  const Socket to_client = server.Accept();
  std::cerr << "Accepted client connection." << std::endl;

  std::cerr << "Connecting to " << FLAGS_proxy_to_host << ":" << FLAGS_proxy_to_port << std::endl;
  const Socket to_underlying = Socket::Connect(FLAGS_proxy_to_host, FLAGS_proxy_to_port);
  std::cerr << "Connection succeeded." << std::endl;

  // Handle early exit (Ctrl+C or SIGTERM).
  SentinelPipe sentinel;
  g_handle_signal = [&sentinel](int signal) {
    char null_char = '\0';
    write(sentinel.write_fd(), &null_char, sizeof(null_char));
  };
  std::signal(SIGINT, signal_wrapper);
  std::signal(SIGTERM, signal_wrapper);

  const size_t buffer_size = 4096;
  Buffer client_to_underlying(buffer_size), underlying_to_client(buffer_size), scratch(buffer_size);

  fd_set descriptors;
  while (true) {
    FD_ZERO(&descriptors);
    FD_SET(to_client.fd(), &descriptors);
    FD_SET(to_underlying.fd(), &descriptors);
    FD_SET(sentinel.read_fd(), &descriptors);

    const int result = select(3, &descriptors, nullptr, nullptr, nullptr);
    if (result < 0) {
      perror("Select");
      break;
    }

    if (FD_ISSET(to_client.fd(), &descriptors)) {
      // Forward client message to underlying.
      const ssize_t bytes_read = read(to_client.fd(), client_to_underlying.buffer(), buffer_size);
      if (bytes_read < 0) {
        perror("Read from client");
        break;
      }

      ssize_t left_to_write = bytes_read;
      uint8_t* buffer = client_to_underlying.buffer();
      while (left_to_write > 0) {
        const ssize_t bytes_written = write(to_underlying.fd(), buffer, left_to_write);
        if (bytes_written < 0) {
          perror("Write to underlying");
          break;
        }
        left_to_write -= bytes_written;
        buffer += bytes_written;
      }
    }

    if (FD_ISSET(to_underlying.fd(), &descriptors)) {
      // Forward underlying message to client.
      const ssize_t bytes_read = read(to_underlying.fd(), underlying_to_client.buffer(), buffer_size);
      if (bytes_read < 0) {
        perror("Read from underlying");
        break;
      }

      ssize_t left_to_write = bytes_read;
      uint8_t* buffer = underlying_to_client.buffer();
      while (left_to_write > 0) {
        const ssize_t bytes_written = write(to_client.fd(), buffer, left_to_write);
        if (bytes_written < 0) {
          perror("Write to client");
          break;
        }
        left_to_write -= bytes_written;
        buffer += bytes_written;
      }
    }

    if (FD_ISSET(sentinel.read_fd(), &descriptors)) {
      read(sentinel.read_fd(), scratch.buffer(), 1);
      break;
    }
  }

  std::cerr << "Done and exiting." << std::endl;
  return 0;
}
