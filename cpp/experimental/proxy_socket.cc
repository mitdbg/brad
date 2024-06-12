#include <iostream>
#include <stdexcept>

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

  // TODO: Shuffle bytes between the connections.

  return 0;
}
