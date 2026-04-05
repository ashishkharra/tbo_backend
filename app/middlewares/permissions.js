export const requirePermission = (permission) => {
    return (req, res, next) => {
      // if (!req.user) {
      //   return res.status(401).json({
      //     success: false,
      //     message: 'Authentication required'
      //   });
      // }
  
      // if (!req.user.hasPermission(permission)) {
      //   return res.status(403).json({
      //     success: false,
      //     message: 'Insufficient permissions'
      //   });
      // }
  
      next();
    };
  };