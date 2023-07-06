/// Hold some type, but prevent any access to it
pub struct Seal<T>(T);

impl<T> Seal<T> {
    pub fn new(t: T) -> Self {
        Seal(t)
    }
}
