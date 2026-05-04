pub type Handle

pub type Error {
  NotFound
  AccessDenied
  IsDirectory
  Unknown(reason: String)
}

@external(erlang, "fcgi_ffi", "validate_file")
pub fn validate_file(path: String) -> Result(Nil, Error)

@external(erlang, "fcgi_ffi", "open")
pub fn open(path: String) -> Result(Handle, Error)

@external(erlang, "fcgi_ffi", "pread")
pub fn pread(handle: Handle, offset: Int, size: Int) -> Result(BitArray, Error)

@external(erlang, "fcgi_ffi", "close")
pub fn close(handle: Handle) -> Nil
