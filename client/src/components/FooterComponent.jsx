import facebook from "@/assets/social/facebook-white.svg"
import instagram from "@/assets/social/instagram-white.svg"
import twitter from "@/assets/social/twitter-white.svg"
import app from "@/assets/store/app-store.svg"
import play from "@/assets/store/play-store.svg"
import windows from "@/assets/store/windows-store.svg"

const FooterComponent = () => {
  return <>
    <footer className="d-flex flex-wrap justify-content-between align-items-center  border-top bg-black">
    <div className="m-10" >
    <ul className="nav justify-content-end ">
    <li className="nav-item"><a href="#" className="nav-link px-2 text-light"> Home |</a></li>
      <li className="nav-item"><a href="#" className="nav-link px-2 text-light"> Terms and condition |</a></li>
      <li className="nav-item"><a href="#" className="nav-link px-2 text-light"> Privacy Policy |</a></li>
      <li className="nav-item"><a href="#" className="nav-link px-2 text-light"> Collection Statement |</a></li>
      <li className="nav-item"><a href="#" className="nav-link px-2 text-light"> Help |</a></li>
      <li className="nav-item"><a href="#" className="nav-link px-2 text-light"> Manage Account</a></li>
    </ul>
    <p className="mb-0 text-light">Copyright @ 2016 DEMO Streaming. All. Rights Reserved.</p>
     <img src={facebook}  className="p-2"  alt="" />
     <img src={twitter}  className= "p-4" alt="" />
     <img src={instagram} className="p-4"  alt=""/>
    </div>
    <div className="d-flex justify-content-center">
    <img src={app}  className="m-2"  alt="" />
    <img src={play}  className= " m-2" alt="" />
    <img src={windows} className=" m-2" alt=""/>
     </div>

  </footer></>
};

export default FooterComponent;





