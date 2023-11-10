const HeaderComponent = () => {
  return (
    <>
    <nav className="navbar navbar-expand-lg bg-primary">
  <div className="container-fluid">
    <a className="navbar-brand text-light" href="#">DEMO Streaming</a>
      <form className="d-flex" role="search">
      <a className="navbar-brand text-light" href="#">Log in</a>
        <button className="btn btn-dark " type="submit">Start your free trial</button>
      </form>
    </div>
</nav>
<nav className="navbar navbar-expand-lg bg-dark">
  <div className="container-fluid">
    <a className="navbar-brand text-light" href="#">Popolar Title</a>
    </div>
</nav>


    </>

  );
};

export default HeaderComponent;





