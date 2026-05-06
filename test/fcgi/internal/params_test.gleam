import fcgi/internal/connection
import gleam/dict
import gleam/http
import gleam/http/request
import gleam/option

pub fn maps_all_recognised_params_test() {
  let p =
    dict.from_list([
      #("REQUEST_METHOD", "POST"),
      #("SERVER_NAME", "example.test"),
      #("SERVER_PORT", "8443"),
      #("HTTPS", "on"),
      #("PATH_INFO", "/things/42"),
      #("QUERY_STRING", "x=1&y=2"),
      #("CONTENT_TYPE", "application/json"),
      #("CONTENT_LENGTH", "17"),
      #("HTTP_USER_AGENT", "curl/8.0"),
      #("HTTP_X_FORWARDED_FOR", "10.0.0.1"),
      #("DOCUMENT_ROOT", "/var/www"),
    ])
  let assert Ok(req) = connection.to_http_request(p, "BODY")
  assert req.method == http.Post
  assert req.host == "example.test"
  assert req.port == option.Some(8443)
  assert req.scheme == http.Https
  assert req.path == "/things/42"
  assert req.query == option.Some("x=1&y=2")
  assert req.body == "BODY"
  assert request.get_header(req, "content-type") == Ok("application/json")
  assert request.get_header(req, "content-length") == Ok("17")
  assert request.get_header(req, "user-agent") == Ok("curl/8.0")
  assert request.get_header(req, "x-forwarded-for") == Ok("10.0.0.1")
  assert request.get_header(req, "document-root") == Error(Nil)
}

pub fn defaults_when_optional_params_missing_test() {
  let p = dict.from_list([#("REQUEST_METHOD", "GET")])
  let assert Ok(req) = connection.to_http_request(p, "")
  assert req.method == http.Get
  assert req.host == "localhost"
  assert req.port == option.None
  assert req.scheme == http.Http
  assert req.path == "/"
  assert req.query == option.None
}

pub fn https_off_resolves_to_http_scheme_test() {
  let p = dict.from_list([#("REQUEST_METHOD", "GET"), #("HTTPS", "off")])
  let assert Ok(req) = connection.to_http_request(p, "")
  assert req.scheme == http.Http
}

pub fn https_empty_resolves_to_http_scheme_test() {
  let p = dict.from_list([#("REQUEST_METHOD", "GET"), #("HTTPS", "")])
  let assert Ok(req) = connection.to_http_request(p, "")
  assert req.scheme == http.Http
}

pub fn http_header_with_multiple_underscores_lowercases_and_dashes_test() {
  let p =
    dict.from_list([
      #("REQUEST_METHOD", "GET"),
      #("HTTP_X_FOO_BAR", "value"),
    ])
  let assert Ok(req) = connection.to_http_request(p, "")
  assert request.get_header(req, "x-foo-bar") == Ok("value")
}

pub fn http_header_with_empty_value_is_preserved_test() {
  let p = dict.from_list([#("REQUEST_METHOD", "GET"), #("HTTP_X_EMPTY", "")])
  let assert Ok(req) = connection.to_http_request(p, "")
  assert request.get_header(req, "x-empty") == Ok("")
}

pub fn http_header_with_crlf_value_passes_through_test() {
  let p =
    dict.from_list([
      #("REQUEST_METHOD", "GET"),
      #("HTTP_X_DANGEROUS", "ok\r\nX-Injected: bad"),
    ])
  let assert Ok(req) = connection.to_http_request(p, "")
  assert request.get_header(req, "x-dangerous") == Ok("ok\r\nX-Injected: bad")
}

pub fn http_host_overrides_server_name_test() {
  let p =
    dict.from_list([
      #("REQUEST_METHOD", "GET"),
      #("SERVER_NAME", "upstream.local"),
      #("HTTP_HOST", "public.example"),
    ])
  let assert Ok(req) = connection.to_http_request(p, "")
  assert req.host == "public.example"
  assert req.port == option.None
}

pub fn http_host_with_port_overrides_server_port_test() {
  let p =
    dict.from_list([
      #("REQUEST_METHOD", "GET"),
      #("SERVER_NAME", "upstream.local"),
      #("SERVER_PORT", "3000"),
      #("HTTP_HOST", "public.example:8080"),
    ])
  let assert Ok(req) = connection.to_http_request(p, "")
  assert req.host == "public.example"
  assert req.port == option.Some(8080)
}

pub fn http_host_with_unparseable_port_keeps_server_port_test() {
  let p =
    dict.from_list([
      #("REQUEST_METHOD", "GET"),
      #("SERVER_PORT", "3000"),
      #("HTTP_HOST", "public.example:abc"),
    ])
  let assert Ok(req) = connection.to_http_request(p, "")
  assert req.host == "public.example"
  assert req.port == option.Some(3000)
}

pub fn http_host_ipv6_with_port_strips_brackets_test() {
  let p =
    dict.from_list([
      #("REQUEST_METHOD", "GET"),
      #("SERVER_PORT", "3000"),
      #("HTTP_HOST", "[::1]:8080"),
    ])
  let assert Ok(req) = connection.to_http_request(p, "")
  assert req.host == "::1"
  assert req.port == option.Some(8080)
}

pub fn http_host_ipv6_without_port_strips_brackets_test() {
  let p =
    dict.from_list([
      #("REQUEST_METHOD", "GET"),
      #("SERVER_PORT", "3000"),
      #("HTTP_HOST", "[::1]"),
    ])
  let assert Ok(req) = connection.to_http_request(p, "")
  assert req.host == "::1"
  assert req.port == option.Some(3000)
}

pub fn falls_back_to_server_name_when_http_host_empty_test() {
  let p =
    dict.from_list([
      #("REQUEST_METHOD", "GET"),
      #("SERVER_NAME", "example.test"),
      #("HTTP_HOST", ""),
    ])
  let assert Ok(req) = connection.to_http_request(p, "")
  assert req.host == "example.test"
}

pub fn invalid_server_port_resolves_to_none_test() {
  let p = dict.from_list([#("REQUEST_METHOD", "GET"), #("SERVER_PORT", "abc")])
  let assert Ok(req) = connection.to_http_request(p, "")
  assert req.port == option.None
}

pub fn rejects_missing_method_test() {
  let p = dict.from_list([#("SERVER_NAME", "x")])
  let assert Error(err) = connection.to_http_request(p, "")
  assert err == connection.MissingMethod
}

pub fn rejects_invalid_method_test() {
  let p = dict.from_list([#("REQUEST_METHOD", "")])
  let assert Error(err) = connection.to_http_request(p, "")
  assert err == connection.InvalidMethod("")
}
