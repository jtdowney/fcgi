import gleam/dict.{type Dict}
import gleam/http
import gleam/http/request.{type Request}
import gleam/int
import gleam/list
import gleam/option
import gleam/result
import gleam/string

pub type Error {
  MissingMethod
  InvalidMethod(method: String)
}

pub fn to_http_request(
  env: Dict(String, String),
  body: body,
) -> Result(Request(body), Error) {
  use method_str <- result.try(
    dict.get(env, "REQUEST_METHOD")
    |> result.replace_error(MissingMethod),
  )
  use method <- result.try(
    http.parse_method(method_str)
    |> result.replace_error(InvalidMethod(method_str)),
  )

  let scheme = case dict.get(env, "HTTPS") {
    Ok(value) -> https_scheme_from_value(value)
    Error(_) -> http.Http
  }

  let host = result.unwrap(dict.get(env, "SERVER_NAME"), "localhost")
  let path = result.unwrap(dict.get(env, "PATH_INFO"), "/")
  let port =
    dict.get(env, "SERVER_PORT")
    |> result.try(int.parse)
    |> option.from_result
  let query =
    dict.get(env, "QUERY_STRING")
    |> option.from_result

  let headers = build_headers(env)

  Ok(request.Request(
    method:,
    headers:,
    body:,
    scheme:,
    host:,
    port:,
    path:,
    query:,
  ))
}

fn https_scheme_from_value(value: String) -> http.Scheme {
  case string.lowercase(value) {
    "" | "off" -> http.Http
    _ -> http.Https
  }
}

fn build_headers(env: Dict(String, String)) -> List(#(String, String)) {
  env
  |> dict.to_list
  |> list.filter_map(map_header)
}

fn map_header(pair: #(String, String)) -> Result(#(String, String), Nil) {
  let #(key, value) = pair
  case key {
    "CONTENT_TYPE" -> Ok(#("content-type", value))
    "CONTENT_LENGTH" -> Ok(#("content-length", value))
    "HTTP_" <> name ->
      Ok(#(string.replace(string.lowercase(name), "_", "-"), value))
    _ -> Error(Nil)
  }
}
