fn main() -> Result<(), Box<dyn std::error::Error>> {

    //tonic_build::compile_protos("proto/Flight.proto")?;
    //tonic_build::compile_protos("proto/FlightSql.proto")?;

    let out_dir = "./src";

    tonic_build::configure()
        .out_dir(out_dir)
        .compile(&["proto/Flight.proto"], &["proto"])?;

    tonic_build::configure()
        .out_dir(out_dir)
        .compile(&["proto/FlightSql.proto"], &["proto"])?;

    Ok(())
}