#pragma once

namespace brad {

struct ServerInfo {
  std::string host;
  int port;
};

class BradFrontEnd {
 public:
  void AddServer(const std::string &host, int port);

  arrow::Status ExecuteQuery(const std::string &query);

 private:
  std::vector<ServerInfo> server_info_objects_;
};

} // namespace brad
